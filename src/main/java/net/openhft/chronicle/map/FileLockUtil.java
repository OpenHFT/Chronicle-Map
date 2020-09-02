package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleFileLockException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ConcurrentHashMap;

public final class FileLockUtil {

    private static final ConcurrentHashMap<File, FileLockReference> FILE_LOCKS = new ConcurrentHashMap<>();

    private FileLockUtil() { }

    public static void acquireSharedFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
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
    }

    public static void acquireExclusiveFileLock(@NotNull final File canonicalFile, @NotNull final FileChannel channel) {
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
    }

    public static void releaseFileLock(@NotNull final File canonicalFile) {
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

    public static void runExclusively(@NotNull final File canonicalFile,
                                      @NotNull final FileChannel fileChannel,
                                      @NotNull final Runnable fileIOAction) {

        // Atomically acquire a FileLockReference
        final FileLockReference fileLockRef = FILE_LOCKS.compute(canonicalFile, (f, flr) ->
                {
                    if (flr != null)
                        throw new ChronicleFileLockException("A file lock instance already exists for the file " + canonicalFile);
                    try {
                        return new FileLockReference(fileChannel.lock());
                    } catch (IOException e) {
                        throw new ChronicleFileLockException(e);
                    }
                }
        );

        try {
            fileIOAction.run();
        } finally {
            try {
                fileLockRef.release();
            } catch (Exception ignore) {
            } finally {
                FILE_LOCKS.remove(canonicalFile);
            }
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

}