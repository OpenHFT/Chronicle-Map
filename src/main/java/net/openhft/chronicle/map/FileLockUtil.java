package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleFileLockException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FileLockUtil {

    /**
     * Java file locks are maintained on a per JVM basis. So we need to manage them.
     */
    private static final String DISABLE_LOCKING = "chronicle.map.disable.locking";
    private static final ConcurrentHashMap<File, FileLockReference> FILE_LOCKS = new ConcurrentHashMap<>();
    private static final boolean USE_EXCLUSIVE_LOCKING = !Jvm.getBoolean(DISABLE_LOCKING);
    private static final boolean USE_SHARED_LOCKING = !OS.isWindows() && !Jvm.getBoolean(DISABLE_LOCKING) &&
            !"shared".equalsIgnoreCase(System.getProperty(DISABLE_LOCKING));
    private static final AtomicBoolean LOCK_WARNING_PRINTED = new AtomicBoolean();

    private FileLockUtil() {
    }

    public static void acquireSharedFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
        if (USE_SHARED_LOCKING)
            FILE_LOCKS.compute(canonicalFile, (f, flr) ->
                    {
                        try {
                            if (flr == null)
                                return new FileLockReference(channel.lock(0, Long.MAX_VALUE, true));
                            else {
                                if (!flr.fileLock.isShared()) {
                                    throw newUnableToAcquireSharedFileLockException(canonicalFile, null);
                                }
                                flr.reserve();
                                return flr; // keep the old one
                            }
                        } catch (IOException e) {
                            throw newUnableToAcquireSharedFileLockException(canonicalFile, e);
                        }
                    }
            );
        else
            printWarningTheFirstTime();

    }

    public static void acquireExclusiveFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
        if (USE_EXCLUSIVE_LOCKING)
            FILE_LOCKS.compute(canonicalFile, (f, flr) ->
                    {
                        if (flr == null) {
                            try {
                                final FileLock fileLock = channel.lock(0, Long.MAX_VALUE, false);
                                return new FileLockReference(fileLock);
                            } catch (IOException e) {
                                throw newUnableToAcquireExclusiveFileLockException(canonicalFile, e);
                            }
                        } else {
                            throw newUnableToAcquireExclusiveFileLockException(canonicalFile, null);
                        }

                    }
            );
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

    /**
     * @deprecated Use {@link #releaseExclusiveFileLock(File)} or {@link #releaseSharedFileLock(File)} instead.
     */
    @Deprecated
    public static void releaseFileLock(@NotNull final File canonicalFile) {
        releaseExclusiveFileLock(canonicalFile);
    }

    private static void releaseFileLock0(@NotNull File canonicalFile) {
        FILE_LOCKS.compute(canonicalFile, (f, flr) ->
                {
                    if (flr == null)
                        throw new ChronicleFileLockException("Trying to release lock on file " + canonicalFile + " that did not exist");
                    else {
                        final int cnt = flr.release();
                        if (cnt == 0)
                            return null; // Remove the old one
                        else
                            return flr;
                    }
                }

        );
    }

    /**
     * Tries to execute a closure under exclusive file lock.
     * If USE_LOCKING is false, provides synchronization only within local JVM.
     *
     * @param fileIOAction Closure to run, can throw {@link IOException}s.
     * @return <code>true</code> if the lock was successfully acquired and IO action was executed, <code>false</code> otherwise.
     */
    public static boolean tryRunExclusively(@NotNull final File canonicalFile,
                                      @NotNull final FileChannel fileChannel,
                                      @NotNull final FileIOAction fileIOAction) {
        AtomicBoolean locked = new AtomicBoolean(false);

        FILE_LOCKS.compute(canonicalFile, (f, flr) -> {
                    if (flr != null)
                        return flr;

                    try {
                        if (USE_EXCLUSIVE_LOCKING) {
                            try (FileLock ignored = fileChannel.tryLock()) {
                                if (ignored == null)
                                    return null;

                                fileIOAction.fileIOAction();

                                locked.set(true);
                            }
                            catch (OverlappingFileLockException ignored) {
                                // File lock is being held by this JVM, unsuccessful attempt.
                            }
                        } else {
                            fileIOAction.fileIOAction();

                            locked.set(true);
                        }

                        return null;
                    } catch (Exception e) {
                        throw Jvm.rethrow(e);
                    }
                }
        );

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
        FILE_LOCKS.compute(canonicalFile, (f, flr) -> {
                    if (flr != null)
                        throw new ChronicleFileLockException("A file lock instance already exists for the file " + canonicalFile);

                    try {
                        if (USE_EXCLUSIVE_LOCKING) {
                            try (FileLock ignored = fileChannel.lock()) {
                                fileIOAction.fileIOAction();
                            }
                        } else {
                            fileIOAction.fileIOAction();
                        }

                        return null;
                    } catch (Exception e) {
                        throw Jvm.rethrow(e);
                    }
                }
        );
    }

    static void dump() {
        System.out.println(FILE_LOCKS);
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
            Jvm.warn().on(FileLockUtil.class, "File locking is disabled or not supported on this platform (" + System.getProperty("os.name") + "). " +
                    "Make sure you are not running ChronicleMapBuilder::*recover* methods when other processes or threads have the mapped file open!");
        }
    }

    // This class is not thread-safe but instances
    // are protected by means of the FILE_LOCKS map
    private static final class FileLockReference {
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
    interface FileIOAction {
        void fileIOAction() throws IOException;
    }
}