/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.*;

/**
 * ChronicleHashResources is Runnable to be passed as "hunk" to {@link sun.misc.Cleaner}.
 */
public abstract class ChronicleHashResources implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleHashResources.class);

    /**
     * closed field is volatile and {@link #closed()} accessor is not synchronized, because it
     * is called on each ChronicleMap access from all threads, synchronization would make closed()
     * a bottleneck.
     */
    private volatile boolean closed = false;
    private List<MemoryResource> memoryResources = new ArrayList<>();
    private List<Closeable> closeables = new ArrayList<>(1);
    private ArrayList<WeakReference<ContextHolder>> contexts = new ArrayList<>();
    /**
     * Identity String of the ChronicleHash, for which this ChronicleHashResources is created.
     * ChronicleHash couldn't be directly referenced, because it's a hunk for {@link
     * sun.misc.Cleaner}, and it would prevent the chronicleHash from ever becoming unreachable.
     */
    private String chronicleHashIdentityString;

    List<MemoryResource> memoryResources() {
        return memoryResources;
    }

    List<WeakReference<ContextHolder>> contexts() {
        return contexts;
    }

    final synchronized long totalMemory() {
        if (closed)
            return 0L;
        long totalMemory = 0L;
        for (MemoryResource memoryResource : memoryResources) {
            totalMemory += memoryResource.size;
        }
        return totalMemory;
    }

    final boolean closed() {
        return closed;
    }

    private void checkOpen() {
        if (closed)
            throw new ChronicleHashClosedException(chronicleHashIdentityString);
    }

    public final synchronized void setChronicleHashIdentityString(
            String chronicleHashIdentityString) {
        this.chronicleHashIdentityString = chronicleHashIdentityString;
    }

    final synchronized void addMemoryResource(long address, long size) {
        checkOpen();
        memoryResources.add(new MemoryResource(address, size));
    }

    public final synchronized void addCloseable(Closeable closeable) {
        checkOpen();
        closeables.add(closeable);
    }

    final synchronized void addContext(ContextHolder contextHolder) {
        checkOpen();
        expungeStateContexts();
        contexts.add(new WeakReference<>(contextHolder));
    }

    private void expungeStateContexts() {
        contexts.removeIf(ref -> {
            ContextHolder contextHolder = ref.get();
            return contextHolder == null || !contextHolder.get().owner().isAlive();
        });
    }

    /**
     * This run() method is called from {@link sun.misc.Cleaner}, hence must not fail
     */
    @Override
    public void run() {
        Throwable thrown = null;
        try {
            if (closed)
                return;
            try {
                LOG.error("{} not closed manually, cleaned up from Cleaner",
                        chronicleHashIdentityString);
            } catch (Throwable t) {
                thrown = t;
            } finally {
                synchronized (this) {
                    if (closed) {
                        LOG.error("Somebody closed {} while it is processed by Cleaner, " +
                                "this should be impossible", chronicleHashIdentityString);
                    } else {
                        thrown = Throwables.returnOrSuppress(thrown, releaseEverything());
                    }
                }
                if (thrown != null) {
                    try {
                        LOG.error("Error on releasing resources of " + chronicleHashIdentityString,
                                thrown);
                    } catch (Throwable t) {
                        // This may occur if we are in shutdown hooks, and the log service has
                        // already been shut down. Try to fall back to printStackTrace().
                        thrown.addSuppressed(t);
                        thrown.printStackTrace();
                    }
                }
            }
        } catch (Throwable ignore) {
            // just don't fail anyway
        }
    }

    abstract Throwable releaseSystemResources();

    public final boolean releaseManually() {
        if (closed)
            return false;
        synchronized (this) {
            if (closed)
                return false;
            Throwable thrown = releaseEverything();
            if (thrown != null)
                throw Throwables.propagate(thrown);
            return true;
        }
    }

    private Throwable releaseEverything() {
        // It's important to set closed = true before calling closeContexts(), to ensure that
        // threads which access this chronicleHash for the first time concurrently with
        // chronicleHash.close() will either fail to register the context via addContext() (because
        // checkOpen() is called there), or either the registered contexts will be visible and
        // closed in closeContexts()
        closed = true;

        // Paranoiac mode: methods closeContexts(), closeCloseables(), releaseSystemResources() and
        // Throwables.returnOrSuppress() should never throw any throwables (the first three should
        // return them instead of throwing), but we don't trust ourselves and wrap every call into
        // it's own try-finally block
        try {
            Throwable thrown = null;
            try {
                thrown = closeContexts();
            } finally {
                try {
                    thrown = Throwables.returnOrSuppress(thrown, closeCloseables());
                } finally {
                    // It's important to close the system resources after contexts and closeables
                    // because the latter may use those system resources
                    thrown = Throwables.returnOrSuppress(thrown, releaseSystemResources());
                }
            }
            return thrown;
        } finally {
            // Make GC life easier
            closeables = null;
            contexts = null;
            memoryResources = null;
        }
    }

    private Throwable closeContexts() {
        Throwable thrown = null;
        // Indexed loop instead of for-each because we may be in the context of cleaning up after
        // OutOfMemoryError, and allocating Iterator object may lead to another OutOfMemoryError, so
        // we try to avoid any allocations
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < contexts.size(); i++) {
            WeakReference<ContextHolder> contextHolderRef = contexts.get(i);
            try {
                ContextHolder contextHolder = contextHolderRef.get();
                if (contextHolder != null) {
                    closeContext(contextHolder);
                }
            } catch (Throwable t) {
                thrown = Throwables.returnOrSuppress(thrown, t);
            }
        }
        return thrown;
    }

    private void closeContext(ContextHolder contextHolder) {
        ChainingInterface context = contextHolder.get();
        try {
            if (context.owner().isAlive()) {
                // Ensures that if the thread owning this context will come to access chronicleHash
                // concurrently with resource releasing operation, it will fail due to the check in
                // context.lockContextLocally() method. If the thread owning this context is
                // currently accessing chronicleHash, closeContext() will spin-wait until the end of
                // this access session.
                context.closeContext(chronicleHashIdentityString);
            }
        } finally {
            // See ContextHolder's class-level Javadoc comment which explains motivation for this
            contextHolder.clear();
        }
    }

    private Throwable closeCloseables() {
        Throwable thrown = null;
        // Indexed loop instead of for-each because we may be in the context of cleaning up after
        // OutOfMemoryError, and allocating Iterator object may lead to another OutOfMemoryError, so
        // we try to avoid any allocations
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < closeables.size(); i++) {
            Closeable closeable = closeables.get(i);
            try {
                closeable.close();
            } catch (Throwable t) {
                thrown = Throwables.returnOrSuppress(thrown, t);
            }
        }
        return thrown;
    }
}
