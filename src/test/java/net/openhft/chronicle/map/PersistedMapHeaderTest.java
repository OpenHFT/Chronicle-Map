/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static net.openhft.chronicle.hash.impl.SizePrefixedBlob.SELF_BOOTSTRAPPING_HEADER_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PersistedMapHeaderTest {
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    //! Persisted startup must reject a short header with the measured file size in its diagnostic.
    // Keep the ready size word intact so this exercises the header length check without a timeout.
    // Closing the map and draining deferred unmapping before truncation preserves Windows ownership.
    @Test
    public void truncatedHeaderReportsItsActualFileSize() throws IOException {
        File file = temporaryFolder.newFile("short-header.map");
        ChronicleMapBuilder<Long, Long> builder = ChronicleMap.of(Long.class, Long.class).entries(10);
        try (ChronicleMap<Long, Long> map = builder.createPersistedTo(file)) {
            map.put(1L, 42L);
        }
        BackgroundResourceReleaser.releasePendingResources();
        long truncatedSize = SELF_BOOTSTRAPPING_HEADER_OFFSET + 1;
        try (RandomAccessFile owner = new RandomAccessFile(file, "rw")) {
            owner.setLength(truncatedSize);
        }

        IOException failure = assertThrows(IOException.class, () -> builder.createPersistedTo(file).close());
        assertTrue(failure.getMessage(), failure.getMessage().contains("The file is shorter than the header size:"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("file size: " + truncatedSize));
        assertTrue(failure.getMessage(), failure.getMessage().contains(file.getName()));
    }

    //! A complete self-bootstrapping header does not imply a complete mutable-state block.
    // Exercise the positional read's EOF branch and preserve the file-specific truncation failure.
    // TemporaryFolder.assureDeletion also checks that a failed open releases its file owner.
    @Test
    public void truncatedGlobalStateIsRejectedBeforeMapping() throws IOException {
        File file = temporaryFolder.newFile("short-global-state.map");
        ChronicleMapBuilder<Long, Long> builder = ChronicleMap.of(Long.class, Long.class).entries(10);
        long headerSize;
        try (ChronicleMap<Long, Long> map = builder.createPersistedTo(file)) {
            headerSize = ((VanillaChronicleHash<?, ?, ?, ?>) map).headerSize;
            map.put(1L, 42L);
        }
        BackgroundResourceReleaser.releasePendingResources();
        try (RandomAccessFile owner = new RandomAccessFile(file, "rw")) {
            owner.setLength(headerSize + Long.BYTES + 1);
        }

        IOException failure = assertThrows(IOException.class, () -> builder.createPersistedTo(file).close());
        assertTrue(failure.getMessage(), failure.getMessage().contains("truncated"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(file.getName()));
    }

    @Test
    public void createReopenAndRecoverPreserveEntries() throws IOException {
        File file = temporaryFolder.newFile("header.map");
        ChronicleMapBuilder<Long, Long> builder = ChronicleMap.of(Long.class, Long.class).entries(10);

        try (ChronicleMap<Long, Long> created = builder.createPersistedTo(file)) {
            created.put(1L, 42L);
            assertEquals(Long.valueOf(42L), created.get(1L));
        }
        try (ChronicleMap<Long, Long> reopened = builder.createPersistedTo(file)) {
            assertEquals(Long.valueOf(42L), reopened.get(1L));
            reopened.put(2L, 43L);
        }
        try (ChronicleMap<Long, Long> recovered = builder.recoverPersistedTo(file, true)) {
            assertEquals(2, recovered.size());
            assertEquals(Long.valueOf(42L), recovered.get(1L));
            assertEquals(Long.valueOf(43L), recovered.get(2L));
        }
        try (ChronicleMap<Long, Long> reopened = builder.createPersistedTo(file)) {
            assertEquals(2, reopened.size());
            assertEquals(Long.valueOf(42L), reopened.get(1L));
            assertEquals(Long.valueOf(43L), reopened.get(2L));
        }
    }
}
