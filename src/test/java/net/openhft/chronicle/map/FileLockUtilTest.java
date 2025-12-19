/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleFileLockException;
import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FileLockUtilTest {

    private File canonicalFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        canonicalFile = new File("file.lock").getCanonicalFile();
        canonicalFile.delete();
        canonicalFile.createNewFile();
        RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(canonicalFile);
        fileChannel = raf.getChannel();
    }

    @AfterEach
    public void cleanup() throws IOException {
        fileChannel.close();
        CanonicalRandomAccessFiles.release(canonicalFile);
    }

    @Test
    public void testShared() {
        assertDoesNotThrow(() -> {
            if (!OS.isWindows()) {
                CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
                CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
                CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
                CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            }
        }, "shared lock acquire/release");
    }

    @Test
    public void testExclusiveNormalCase() {
        assertDoesNotThrow(() -> {
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        }, "exclusive lock acquire/release (normal case)");
    }

    @Test
    public void testTryExclusiveButWasShared() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
                fail("fail");
            } catch (ChronicleFileLockException e) {
                assertNotNull(e, "exception should be thrown when acquiring exclusive lock while shared lock is held");
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
                fail("fail");
            } catch (ChronicleFileLockException e) {
                assertNotNull(e, "exception should be thrown when acquiring shared lock while exclusive lock is held");
            }
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        }
    }

    @Test
    public void testComplicated() {
        assertDoesNotThrow(() -> {
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
        }, "mixed lock sequence");
    }

    @Test
    public void testRunExclusively() {
        final AtomicInteger cnt = new AtomicInteger();
        CanonicalRandomAccessFiles.runExclusively(canonicalFile, fileChannel, cnt::incrementAndGet);
        assertEquals(1, cnt.get(), "runnable should be executed exactly once when run exclusively");
    }

    @Test
    public void testRunExclusivelyButUsed() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            try {
                CanonicalRandomAccessFiles.runExclusively(canonicalFile, fileChannel, () -> {
                });
                fail("fail");
            } catch (ChronicleFileLockException e) {
                CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
                assertNotNull(e, "exception should be thrown when attempting to run exclusively while file is locked");
            }
        }
    }

    @Test
    public void testTryRunExclusively() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);

            boolean lockedAndRun = CanonicalRandomAccessFiles.tryRunExclusively(canonicalFile, fileChannel, () -> {
            });

            assertFalse(lockedAndRun, "tryRunExclusively should return false when file is already locked");

            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);

            lockedAndRun = CanonicalRandomAccessFiles.tryRunExclusively(canonicalFile, fileChannel, () -> {
            });

            assertTrue(lockedAndRun, "tryRunExclusively should return true when file is not locked");
        }
    }
}
