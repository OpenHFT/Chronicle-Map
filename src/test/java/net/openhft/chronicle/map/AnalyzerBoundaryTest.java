/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.algo.XxHash;
import net.openhft.chronicle.hash.impl.InternalMapFileAnalyzer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

class AnalyzerBoundaryTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 16})
    void analysingStringMapReleasesMappingSoFileCanBeDeleted(int segments) throws Exception {
        Path file = directory.resolve("inspect.map");
        try (ChronicleMap<String, String> map = ChronicleMap.of(String.class, String.class)
                .entries(16).actualSegments(segments).averageKeySize(8).averageValueSize(8).createPersistedTo(file.toFile())) {
            map.put("sample", "value");
        }
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream captured = new PrintStream(output)) {
            System.setOut(captured);
            InternalMapFileAnalyzer.main(new String[]{file.toString()});
            assertTrue(output.toString().contains("sample"), output.toString());
            Files.delete(file);
        } finally {
            System.setOut(original);
            // Permit baseline cleanup after detecting a leaked mapping.
            System.gc();
        }
    }

    @Test
    void nullMapHeaderIsRejectedAndMappingReleased() throws Exception {
        Path file = directory.resolve("null-header.map");
        byte[] header = "!!null \"\"".getBytes(StandardCharsets.UTF_8);
        Bytes<?> headerBytes = Bytes.allocateElasticOnHeap();
        try {
            headerBytes.writeInt(header.length);
            headerBytes.write(header);
            long hash = new XxHash(0).applyAsLong(headerBytes);
            ByteBuffer contents = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + header.length)
                    .order(ByteOrder.LITTLE_ENDIAN);
            contents.putLong(hash).putInt(header.length).put(header);
            Files.write(file, contents.array());
        } finally {
            headerBytes.releaseLast();
        }
        String[] args = {file.toString()};
        IOException failure = assertThrows(IOException.class, () -> InternalMapFileAnalyzer.main(args));
        assertEquals("Map header must not be null", failure.getMessage());
        Files.delete(file);
    }

    @Test
    void truncatedFileAlsoReleasesMapping() throws Exception {
        Path file = directory.resolve("truncated.map");
        Files.write(file, new byte[8]);
        assertThrows(java.nio.BufferUnderflowException.class, () -> InternalMapFileAnalyzer.main(new String[]{file.toString()}));
        Files.delete(file);
    }
}
