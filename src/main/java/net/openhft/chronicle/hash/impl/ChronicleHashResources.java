/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 * ChronicleHashResources is Runnable to be passed as "hunk" to {@link sun.misc.Cleaner}.
 */
public abstract class ChronicleHashResources implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleHashResources.class);

    private static final int OPEN = 0;
    private static final int PARTIALLY_CLOSED = 1;
    private static final int COMPLETELY_CLOSED = 2;

    /**
     * This field is volatile and {@link #closed()} accessor is not synchronized, because it
     * is called on each {@link VanillaChronicleHash} access from all threads, synchronization would
     * make {@code closed()} a bottleneck.
     */
    private volatile int state = OPEN;
    private List<MemoryResource> memoryResources = new ArrayList<>();
    private List<Closeable> closeables = new ArrayList<>(1);
    private List<WeakReference<ContextHolder>> contexts = new ArrayList<>();
    /**
     * Identity String of the ChronicleHash, for which this ChronicleHashResources is created.
     * ChronicleHash couldn't be directly referenced, because {@code ChronicleHashResources} is a
     * hunk for {@link sun.misc.Cleaner}, and it would prevent the chronicleHash from ever becoming
     * unreachable.
     */
    private String chronicleHashIdentityString;

    List<WeakReference<ContextHolder>> contexts() {
        return contexts;
    }

    final synchronized long totalMemory() {
        if (closed())
            return 0L;
        long totalMemory = 0L;
        //noinspection ForLoopReplaceableByForEach -- allocation-free looping
        for (int i = 0; i < memoryResources.size(); i++) {
            totalMemory += memoryResources.get(i).size;
        }
        return totalMemory;
    }

    final boolean closed() {
        return state != OPEN;
    }

    private void checkOpen() {
        if (closed())
            throw new ChronicleHashClosedException(chronicleHashIdentityString);
    }

    /**
     * Need to set {@link #chronicleHashIdentityString} via this setter rather than once in the
     * constructor, because {@code ChronicleHashResources} instance is created before it's
     * {@link net.openhft.chronicle.hash.ChronicleHash}.
     */
    public final synchronized void setChronicleHashIdentityString(
            String chronicleHashIdentityString) {
        checkOpen();
        this.chronicleHashIdentityString = chronicleHashIdentityString;
    }

    final synchronized void addMemoryResource(long address, long size) {
        checkOpen();
        memoryResources.add(new MemoryResource(address, size));
    }

    final synchronized void addCloseable(Closeable closeable) {
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
            if (state == COMPLETELY_CLOSED)
                return;
            try {
                LOG.error("{} is not closed manually, cleaned up from Cleaner",
                        chronicleHashIdentityString);
            } catch (Throwable t) {
                thrown = t;
            } finally {
                synchronized (this) {
                    if (state == COMPLETELY_CLOSED) {
                        LOG.error("Somebody closed {} while it is processed by Cleaner, " +
                                "this should be impossible", chronicleHashIdentityString);
                    } else {
                        thrown = Throwables.returnOrSuppress(thrown, releaseEverything(true));
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
            // Just don't fail anyway. We will have another attempt to close this ChronicleMap from
            // ChronicleHashCloseOnExitHook.
        }
    }

    abstract void releaseMemoryResource(MemoryResource memoryResource) throws IOException;

    Throwable releaseExtraSystemResources() {
        // nothing to release
        return null;
    }

    public final boolean releaseManually() {
        if (state == COMPLETELY_CLOSED)
            return false;
        synchronized (this) {
            if (state == COMPLETELY_CLOSED)
                return false;
            Throwable thrown = releaseEverything(false);
            if (thrown != null)
                throw Throwables.propagate(thrown);
            return true;
        }
    }

    private Throwable releaseEverything(boolean releaseFromCleaner) {
        // It's important to set state = PARTIALLY_CLOSED before calling closeContexts(), to ensure
        // that threads which access this chronicleHash for the first time concurrently with
        // chronicleHash.close() will either fail to register the context via addContext() (because
        // checkOpen() is called there), or either the registered contexts will be visible and
        // closed in closeContexts(). Also, it allows to not care about null elements of
        // memoryResources, contexts and closeables lists in addMemoryResource(), addContext() and
        // addCloseable() respectively.
        state = PARTIALLY_CLOSED;

        // Paranoiac mode: methods closeContexts(), closeCloseables(), releaseMemoryResources(),
        // releaseExtraSystemResources() and Throwables.returnOrSuppress() should never throw any
        // throwables (the first three should return them instead of throwing), but we don't trust
        // ourselves and wrap every call into it's own try-finally block.
        Throwable thrown = null;
        try {
            if (contexts != null)
                thrown = closeContexts();
        } catch (Throwable t) {
            thrown = t;
        } finally {
            try {
                if (closeables != null)
                    thrown = Throwables.returnOrSuppress(thrown, closeCloseables());
            } catch (Throwable t) {
                try {
                    thrown = Throwables.returnOrSuppress(thrown, t);
                } catch (Throwable stackOverflowError) {
                    // It seems that only StackOverflowError could be thrown from
                    // Throwables.returnOrSuppress(), but in this case `thrown` and `t` are likely
                    // StackOverflowErrors too, so just return one of them without trying to call
                    // any more methods, including Throwable.addSuppressed().
                    thrown = thrown != null ? thrown : t;
                }
            }
        }
        // It's important to close the system resources only after contexts and closeables are
        // closed successfully, because the latter may use those system resources. However if
        // releaseEverything() is called from Cleaner, the ChronicleMap object is already
        // unreachable, that means no contexts are accessing it anymore and the problem with closing
        // contexts could probably be StackOverflowError or other exotic Error, so in this case we
        // try to close the system resources anyway.
        if (thrown == null || releaseFromCleaner) {
            try {
                thrown = releaseSystemResources(thrown);
            } catch (Throwable stackOverflowError) {
                // It seems that only StackOverflowError could be thrown (rather than returned!)
                // from releaseSystemResources(), so just return any Throwable from
                // releaseEverything() without trying to call any more methods.
                thrown = thrown != null ? thrown : stackOverflowError;
            }
        }
        if (thrown == null) {
            state = COMPLETELY_CLOSED;
        }
        return thrown;
    }

    private Throwable releaseSystemResources(Throwable thrown) {
        try {
            if (memoryResources != null)
                thrown = Throwables.returnOrSuppress(thrown, releaseMemoryResources());
        } catch (Throwable t) {
            thrown = Throwables.returnOrSuppress(thrown, t);
        } finally {
            try {
                thrown = Throwables.returnOrSuppress(thrown, releaseExtraSystemResources());
            } catch (Throwable t) {
                try {
                    thrown = Throwables.returnOrSuppress(thrown, t);
                } catch (Throwable stackOverflowError) {
                    // It seems that only StackOverflowError could be thrown from
                    // Throwables.returnOrSuppress(), but in this case `thrown` and `t` are likely
                    // StackOverflowErrors too, so just return one of them without trying to call
                    // any more methods, including Throwable.addSuppressed().
                    thrown = thrown != null ? thrown : t;
                }
            }
        }
        return thrown;
    }

    private Throwable closeContexts() {
        Throwable thrown = null;
        List<WeakReference<ContextHolder>> contexts = this.contexts;
        // Indexed loop instead of using Iterator because we may be in the context of cleaning up
        // after OutOfMemoryError, and allocating Iterator object may lead to another
        // OutOfMemoryError, so we try to avoid any allocations.
        for (int i = 0; i < contexts.size(); i++) {
            WeakReference<ContextHolder> contextHolderRef = contexts.get(i);
            // The context reference could have already been set to null in (*). See comments in
            // closeContext() with more explanations.
            if (contextHolderRef != null) {
                try {
                    ContextHolder contextHolder = contextHolderRef.get();
                    if (contextHolder != null) {
                        closeContext(contextHolder);
                    }
                    // (*) Make GC life easier
                    contexts.set(i, null);
                } catch (Throwable t) {
                    thrown = Throwables.returnOrSuppress(thrown, t);
                }
            }
        }
        // Forget about contexts only if all of them are successfully closed, i. e. no throwables
        // were thrown in the above loop.
        if (thrown == null) {
            // Make GC life easier
            this.contexts = null;
        }
        return thrown;
    }

    private void closeContext(ContextHolder contextHolder) {
        ChainingInterface context = contextHolder.get();
        // The context could have already been cleared, if this is the second attempt to close
        // contexts, the first one failed e. g. with IllegalStateException on one of the contexts
        // (see comment (*) below in this method), it could have succeed for some contexts and
        // contextHolder.clear() is performed.
        if (context != null) {
            if (context.owner().isAlive()) {
                // Ensures that if the thread owning this context will come to access chronicleHash
                // concurrently with resource releasing operation, it will fail due to the check in
                // context.lockContextLocally() method. If the thread owning this context is
                // currently accessing chronicleHash, closeContext() will spin-wait until the end of
                // this access session.
                context.closeContext(chronicleHashIdentityString);
            }

            // (*) Don't execute contextHolder.clear() from a finally section of a try block
            // wrapping context.closeContext(), because if context.closeContext() fails e. g. with
            // IllegalStateException because the context is currently used (this happens if
            // ChronicleMap.close() is called within try-with-resources block of it's own operation,
            // see MapCloseTest.closeInContextTest()), we may want to try to close this context
            // again from ChronicleHashCloseOnExitHook.

            // See ContextHolder's class-level Javadoc comment that explains the motivation for
            // calling contextHolder.clear().
            contextHolder.clear();
        }
    }

    private Throwable closeCloseables() {
        Throwable thrown = null;
        List<Closeable> closeables = this.closeables;
        // Indexed loop instead of using Iterator because we may be in the context of cleaning up
        // after OutOfMemoryError, and allocating Iterator object may lead to another
        // OutOfMemoryError, so we try to avoid any allocations.
        for (int i = 0; i < closeables.size(); i++) {
            Closeable closeable = closeables.get(i);
            // The closeable could have already been set to null in (*), see comments in
            // closeContexts() method which has similar logic.
            if (closeable != null) {
                try {
                    closeable.close();
                    // (*) Make GC life easier
                    closeables.set(i, null);
                } catch (Throwable t) {
                    thrown = Throwables.returnOrSuppress(thrown, t);
                }
            }
        }
        // Forget about closeables only if all of them are successfully closed, i. e. no throwables
        // were thrown in the above loop.
        if (thrown == null) {
            // Make GC life easier
            this.closeables = null;
        }
        return thrown;
    }

    private Throwable releaseMemoryResources() {
        Throwable thrown = null;
        List<MemoryResource> memoryResources = this.memoryResources;
        // Indexed loop instead of using Iterator because we may be in the context of cleaning up
        // after OutOfMemoryError, and allocating Iterator object may lead to another
        // OutOfMemoryError, so we try to avoid any allocations.
        for (int i = 0; i < memoryResources.size(); i++) {
            MemoryResource memoryResource = memoryResources.get(i);
            // The memory resource could have already been nulled out in (*), see comments in
            // closeCloseables() method which has similar logic.
            if (memoryResource != null) {
                try {
                    releaseMemoryResource(memoryResource);
                    // (*) Make GC life easier
                    memoryResources.set(i, null);
                } catch (Throwable t) {
                    thrown = Throwables.returnOrSuppress(thrown, t);
                }
            }
        }
        // Forget about memory resources only if all of them are successfully released, i. e. no
        // throwables were thrown in the above loop.
        if (thrown == null) {
            this.memoryResources = null;
        }
        return thrown;
    }
}
