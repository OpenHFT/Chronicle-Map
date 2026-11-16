/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class CanonicalHandleFailureTest {
    @TempDir Path directory;

    @Test
    void exclusiveCallbacksDoNotRetainHiddenReferences() throws Exception {
        for (boolean fail : new boolean[]{false, true}) {
            File file = directory.resolve("callback-" + fail).toFile().getCanonicalFile();
            RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(file);
            IOException failure = new IOException("callback failed");
            try {
                if (fail)
                    assertSame(failure, assertThrows(IOException.class, () ->
                            CanonicalRandomAccessFiles.runExclusively(file, raf.getChannel(), () -> { throw failure; })));
                else
                    CanonicalRandomAccessFiles.runExclusively(file, raf.getChannel(), () -> raf.writeInt(17));
                assertTrue(raf.getChannel().isOpen());
                CanonicalRandomAccessFiles.release(file);
                assertFalse(raf.getChannel().isOpen(), "callback must release its temporary reference");
            } finally {
                // Release a leaked baseline reference without leaving an open test handle.
                CanonicalRandomAccessFiles.release(file);
                raf.close();
            }
        }
    }

    @Test
    void failedExclusiveAcquisitionDoesNotRetainHiddenReference() throws Exception {
        File file = directory.resolve("lock").toFile().getCanonicalFile();
        RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(file);
        try {
            CanonicalRandomAccessFiles.acquireExclusiveFileLock(file, raf.getChannel());
            assertThrows(RuntimeException.class, () ->
                    CanonicalRandomAccessFiles.acquireExclusiveFileLock(file, raf.getChannel()));
            CanonicalRandomAccessFiles.releaseExclusiveFileLock(file);
            CanonicalRandomAccessFiles.release(file);
            assertFalse(raf.getChannel().isOpen(), "rejected lock must not increment the file reference count");
        } finally { CanonicalRandomAccessFiles.release(file); raf.close(); }
    }

    @Test
    void tryExclusiveFailurePreservesOwnersHandleAndReleasesTemporaryLock() throws Exception {
        File file = directory.resolve("try-lock").toFile().getCanonicalFile();
        RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(file);
        IOException failure = new IOException("callback failed");
        try {
            assertSame(failure, assertThrows(IOException.class, () ->
                    CanonicalRandomAccessFiles.tryRunExclusively(file, raf.getChannel(), () -> { throw failure; })));
            assertTrue(raf.getChannel().isOpen());
            assertTrue(CanonicalRandomAccessFiles.tryRunExclusively(file, raf.getChannel(), () -> raf.writeInt(23)));
            CanonicalRandomAccessFiles.release(file);
            assertFalse(raf.getChannel().isOpen());
        } finally { CanonicalRandomAccessFiles.release(file); raf.close(); }
    }
}
