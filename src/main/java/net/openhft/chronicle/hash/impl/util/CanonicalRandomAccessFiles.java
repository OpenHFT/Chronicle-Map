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

package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.CleaningRandomAccessFile;
import net.openhft.chronicle.hash.ChronicleFileLockException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class CanonicalRandomAccessFiles {

    /**
     * Java file locks are maintained on a per JVM basis. So we need to manage them.
     */
    private static final String DISABLE_LOCKING = "chronicle.map.disable.locking";
    private static final boolean USE_EXCLUSIVE_LOCKING = !Jvm.getBoolean(DISABLE_LOCKING);
    private static final boolean USE_SHARED_LOCKING = !OS.isWindows() && !Jvm.getBoolean(DISABLE_LOCKING) &&
            !"shared".equalsIgnoreCase(Jvm.getProperty(DISABLE_LOCKING));
    private static final AtomicBoolean LOCK_WARNING_PRINTED = new AtomicBoolean();
    private static final ConcurrentHashMap<File, RafReference> CANONICAL_RAFS = new ConcurrentHashMap<>();

    // https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments
    // @formatter:off
    private static final Consumer<RafReference> NO_OP = rr -> {};
    // @formatter:on

    private CanonicalRandomAccessFiles() {
    }

    public static RandomAccessFile acquire(@NotNull final File file) throws FileNotFoundException {
        return acquire0(file, NO_OP).raf;
    }

    private static RafReference acquire0(@NotNull final File file, Consumer<RafReference> action) {
        return CANONICAL_RAFS.compute(file, (f, ref) -> {
            while (ref != null) {
                try {
                    ref.raf.length();
                } catch (IOException e) {
                    // File is closed by interrupt;
                    break;
                }

                ref.refCount++;
                action.accept(ref);
                return ref;
            }

            try {
                return new RafReference(new CleaningRandomAccessFile(f, "rw"));
            } catch (FileNotFoundException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    public static void release(@NotNull final File file) {
        release0(file, NO_OP);
    }

    private static RafReference release0(@NotNull final File file, Consumer<RafReference> action) {
        return CANONICAL_RAFS.computeIfPresent(file, (f, ref) -> {
            action.accept(ref);
            if (--ref.refCount == 0) {
                try {
                    ref.raf.close();
                } catch (IOException e) {
                    throw Jvm.rethrow(e);
                }
                return null;
            } else {
                return ref;
            }
        });
    }

    public static void acquireSharedFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
        if (USE_SHARED_LOCKING)
            acquire0(canonicalFile, (rafReference) -> {
                try {
                    if (rafReference.lockRef == null)
                        rafReference.lockRef = new FileLockReference(channel.lock(0, Long.MAX_VALUE, true));
                    else {
                        if (!rafReference.lockRef.fileLock.isShared()) {
                            throw newUnableToAcquireSharedFileLockException(canonicalFile, null);
                        }
                        rafReference.lockRef.reserve();
                    }
                } catch (IOException | IllegalStateException e) {
                    throw newUnableToAcquireSharedFileLockException(canonicalFile, e);
                }
            });
        else
            printWarningTheFirstTime();
    }

    public static void acquireExclusiveFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
        if (USE_EXCLUSIVE_LOCKING)
            acquire0(canonicalFile, (rafReference) -> {
                if (rafReference.lockRef == null) {
                    try {
                        final FileLock fileLock = channel.lock(0, Long.MAX_VALUE, false);
                        rafReference.lockRef = new FileLockReference(fileLock);
                    } catch (IOException e) {
                        throw newUnableToAcquireExclusiveFileLockException(canonicalFile, e);
                    }
                } else {
                    throw newUnableToAcquireExclusiveFileLockException(canonicalFile, null);
                }
            });
        else
            printWarningTheFirstTime();
    }

    public static void releaseSharedFileLock(@NotNull final File canonicalFile) {
        if (USE_SHARED_LOCKING)
            releaseFileLock0(canonicalFile);
        else
            printWarningTheFirstTime();
    }

    public static void releaseExclusiveFileLock(@NotNull final File canonicalFile) {
        if (USE_EXCLUSIVE_LOCKING)
            releaseFileLock0(canonicalFile);
        else
            printWarningTheFirstTime();
    }

    private static void releaseFileLock0(@NotNull File canonicalFile) {
        release0(canonicalFile, (rafReference) -> {
            if (rafReference.lockRef == null)
                throw new ChronicleFileLockException("Trying to release lock on file " + canonicalFile + " that did not exist");
            else {
                final int cnt = rafReference.lockRef.release();
                if (cnt == 0)
                    rafReference.lockRef = null;
            }
        });
    }

    /**
     * Tries to execute a closure under exclusive file lock.
     * If USE_LOCKING is false, provides synchronization only within local JVM.
     *
     * @param fileIOAction Closure to run, can throw {@link IOException}s.
     * @return {@code true} if the lock was successfully acquired and IO action was executed, {@code false} otherwise.
     */
    public static boolean tryRunExclusively(@NotNull final File canonicalFile,
                                            @NotNull final FileChannel fileChannel,
                                            @NotNull final FileIOAction fileIOAction) {
        AtomicBoolean locked = new AtomicBoolean(false);

        try {
            acquire0(canonicalFile, (rafReference) -> {
                        if (rafReference.lockRef != null)
                            return;

                        try {
                            if (USE_EXCLUSIVE_LOCKING) {
                                try (FileLock lock = fileChannel.tryLock()) {
                                    if (lock == null) {
                                        rafReference.lockRef = null;
                                        return;
                                    }

                                    fileIOAction.fileIOAction();

                                    locked.set(true);
                                } catch (OverlappingFileLockException ignored) {
                                    // File lock is being held by this JVM, unsuccessful attempt.
                                }
                            } else {
                                fileIOAction.fileIOAction();

                                locked.set(true);
                            }

                            rafReference.lockRef = null;
                        } catch (Exception e) {
                            throw Jvm.rethrow(e);
                        }
                    }
            );
        } finally {
            release(canonicalFile);
        }

        return locked.get();
    }

    /**
     * Executes a closure under exclusive file lock.
     * If USE_LOCKING is false, provides synchronization only within local JVM.
     *
     * @param fileIOAction Closure to run, can throw {@link IOException}s.
     */
    public static void runExclusively(@NotNull final File canonicalFile,
                                      @NotNull final FileChannel fileChannel,
                                      @NotNull final FileIOAction fileIOAction) {
        acquire0(canonicalFile, (rafReference) -> {
            if (rafReference.lockRef != null)
                throw new ChronicleFileLockException("A file lock instance already exists for the file " + canonicalFile);

            try {
                if (USE_EXCLUSIVE_LOCKING) {
                    try (FileLock ignored = fileChannel.lock()) {
                        fileIOAction.fileIOAction();
                    }
                } else {
                    fileIOAction.fileIOAction();
                }
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    static void dump() {
        System.out.println(CANONICAL_RAFS);
    }

    private static ChronicleFileLockException newUnableToAcquireSharedFileLockException(@NotNull final File canonicalFile, @Nullable final Exception e) {
        return new ChronicleFileLockException("Unable to acquire a shared file lock for " + canonicalFile + ". " +
                "Make sure another process is not recovering the map.", e);
    }

    private static ChronicleFileLockException newUnableToAcquireExclusiveFileLockException(@NotNull final File canonicalFile, @Nullable final Exception e) {
        return new ChronicleFileLockException("Unable to acquire an exclusive file lock for " + canonicalFile + ". " +
                "Make sure no other process is using the map.", e);
    }

    private static void printWarningTheFirstTime() {
        if (LOCK_WARNING_PRINTED.compareAndSet(false, true)) {
            Jvm.warn().on(CanonicalRandomAccessFiles.class, "File locking is disabled or not supported on this platform (" + Jvm.getProperty("os.name") + "). " +
                    "Make sure you are not running ChronicleMapBuilder::*recover* methods when other processes or threads have the mapped file open!");
        }
    }

    // This class is not thread-safe but instances
    // are protected by means of the FILE_LOCKS map
    public static final class FileLockReference {
        private final FileLock fileLock;
        private int refCount;

        FileLockReference(@NotNull final FileLock fileLock) {
            this.fileLock = fileLock;
            refCount = 1;
        }

        int reserve() {
            if (refCount == 0)
                throw new IllegalStateException("Ref counter previously released");
            return ++refCount;
        }

        int release() {
            final int cnt = --refCount;
            if (cnt == 0) {
                try {
                    fileLock.release();
                } catch (IOException e) {
                    throw new ChronicleFileLockException(e);
                }
            }
            if (cnt < 0)
                throw new IllegalStateException("Ref counter was " + cnt);

            return cnt;
        }

        @Override
        public String toString() {
            return "FileLockReference{" +
                    "fileLock=" + fileLock +
                    ", refCount=" + refCount +
                    '}';
        }
    }

    @FunctionalInterface
    public interface FileIOAction {
        void fileIOAction() throws IOException;
    }

    // This class is not thread-safe but instances
    // are protected by means of the CANONICAL_RAFS map
    private static final class RafReference {
        private final RandomAccessFile raf;
        private FileLockReference lockRef;
        private int refCount;

        RafReference(@NotNull final RandomAccessFile raf) {
            this.raf = raf;
            this.lockRef = null;
            refCount = 1;
        }

        @Override
        public String toString() {
            return "RafReference{" +
                    "raf=" + raf +
                    ", lockRef=" + lockRef +
                    ", refCount=" + refCount +
                    '}';
        }
    }
}
