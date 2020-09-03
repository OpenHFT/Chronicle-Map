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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
            FileLockUtil.releaseFileLock(canonicalFile);
        }
    }

    @Test
    public void testExclusiveNormalCase() {
        if (!OS.isWindows()) {
            FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
            FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
        }
    }

    @Test
    public void testTryExclusiveButWasShared() {
        if (!OS.isWindows()) {
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
                fail();
            } catch (ChronicleFileLockException ignore) {
            }
            FileLockUtil.releaseFileLock(canonicalFile);
        }
    }

    @Test
    public void testTrySharedButWasExclusive() {
        if (!OS.isWindows()) {
            FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
            try {
                FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
                fail();
            } catch (ChronicleFileLockException ignore) {
            }
            FileLockUtil.releaseFileLock(canonicalFile);
        }
    }

    @Test
    public void testComplicated() {
        if (!OS.isWindows()) {
            FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
            FileLockUtil.releaseFileLock(canonicalFile);
            FileLockUtil.acquireExclusiveFileLock(canonicalFile, fileChannel);
            FileLockUtil.releaseFileLock(canonicalFile);
        }
    }

    @Test
    public void testRunExclusively() {
        if (!OS.isWindows()) {
            final AtomicInteger cnt = new AtomicInteger();
            FileLockUtil.runExclusively(canonicalFile, fileChannel, cnt::incrementAndGet);
            assertEquals(1, cnt.get());
        }
    }

    @Test
    public void testRunExclusivelyButUsed() {
        if (!OS.isWindows()) {
            FileLockUtil.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                FileLockUtil.runExclusively(canonicalFile, fileChannel, () -> {
                });
                fail();
            } catch (ChronicleFileLockException e) {
                FileLockUtil.releaseFileLock(canonicalFile);
            }
        }
    }

}