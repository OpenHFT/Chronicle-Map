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

class FileLockUtilTest {

    private File canonicalFile;
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    @BeforeEach
    void setUp() throws IOException {
        canonicalFile = new File("file.lock").getCanonicalFile();
        canonicalFile.delete();
        canonicalFile.createNewFile();
        raf = CanonicalRandomAccessFiles.acquire(canonicalFile);
        fileChannel = raf.getChannel();
    }

    @AfterEach
    void cleanup() throws IOException {
        fileChannel.close();
        CanonicalRandomAccessFiles.release(canonicalFile);
    }

    @Test
    void testShared() {
        if (!OS.isWindows()) {
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, fileChannel);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
            CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile);
        }
    }

    @Test
    void testExclusiveNormalCase() {
        CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
        CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
        CanonicalRandomAccessFiles.acquireExclusiveFileLock(canonicalFile, fileChannel);
        CanonicalRandomAccessFiles.releaseExclusiveFileLock(canonicalFile);
    }

    @Test
    void testTryExclusiveButWasShared() {
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
    void testTrySharedButWasExclusive() {
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
    void testComplicated() {
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
    void testRunExclusively() {
        final AtomicInteger cnt = new AtomicInteger();
        CanonicalRandomAccessFiles.runExclusively(canonicalFile, fileChannel, cnt::incrementAndGet);
        assertEquals(1, cnt.get());
    }

    @Test
    void testRunExclusivelyButUsed() {
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
    void testTryRunExclusively() {
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
