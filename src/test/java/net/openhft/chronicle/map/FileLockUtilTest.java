package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleFileLockException;
import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FileLockUtilTest {

    private File canonicalFile;
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    @Before
    public void setUp() throws IOException {
        canonicalFile = new File("file.lock").getCanonicalFile();
        canonicalFile.delete();
        canonicalFile.createNewFile();
        raf = CanonicalRandomAccessFiles.acquire(canonicalFile);
        fileChannel = raf.getChannel();
    }

    @After
    public void cleanup() throws IOException {
        fileChannel.close();
        CanonicalRandomAccessFiles.release(canonicalFile);
    }

    @Test
    public void testShared() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
        }
    }

    @Test
    public void testExclusiveNormalCase() {
        CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
        CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
        CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
    }

    @Test
    public void testTryExclusiveButWasShared() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
                fail();
            } catch (ChronicleFileLockException ignore) {
            }
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
        }
    }

    @Test
    public void testTrySharedButWasExclusive() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
            try {
                CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
                fail();
            } catch (ChronicleFileLockException ignore) {
            }
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        }
    }

    @Test
    public void testComplicated() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        }
    }

    @Test
    public void testRunExclusively() {
        final AtomicInteger cnt = new AtomicInteger();
        CanonicalRandomAccessFiles.runExclusively(canonicalFile, fileChannel, cnt::incrementAndGet);
        assertEquals(1, cnt.get());
    }

    @Test
    public void testRunExclusivelyButUsed() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                CanonicalRandomAccessFiles.runExclusively(canonicalFile, fileChannel, () -> {
                });
                fail();
            } catch (ChronicleFileLockException e) {
                CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            }
        }
    }

    @Test
    public void testTryRunExclusively() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);

            boolean lockedAndRun = CanonicalRandomAccessFiles.tryRunExclusively(canonicalFile, fileChannel, () -> {
            });

            assertFalse(lockedAndRun);

            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);

            lockedAndRun = CanonicalRandomAccessFiles.tryRunExclusively(canonicalFile, fileChannel, () -> {
            });

            assertTrue(lockedAndRun);
        }
    }
}