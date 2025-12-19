/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ArrayTest {
    // don't use Double[] as it uses ~3.5x the memory of a double[]
    @Test
    public void testDoubleArray() throws IOException {
        File file = new File(OS.getTarget() + "/testDoubleArray-" + Time.uniqueId());
        double[] a = {2D};
        try (ChronicleMap<Long, double[]> writeMap = ChronicleMapBuilder
                .of(Long.class, double[].class)
                .entries(1_000)
                .averageValue(new double[150])
                .createPersistedTo(file)) {
            writeMap.put(1L, a);
        }

        //read
        try (ChronicleMap<Long, double[]> readMap =
                     ChronicleMapBuilder.of(Long.class, double[].class)
                             .averageValue(new double[150])
                             .createPersistedTo(file)) {
            double[] b = readMap.get(1L);
            assertArrayEquals(a, b, "double[] roundtrip");
        }
    }

    // don't use Long[] as it uses ~3.5x the memory of a long[]
    @Test
    public void testLongArray() throws IOException {
        File file = new File(OS.getTarget() + "/testLongArray-" + Time.uniqueId());
        long[] a = {2};
        try (ChronicleMap<Long, long[]> writeMap = ChronicleMapBuilder
                .of(Long.class, long[].class)
                .entries(1_000)
                .averageValue(new long[150])
                .createPersistedTo(file)) {
            writeMap.put(1L, a);
        }

        //read
        try (ChronicleMap<Long, long[]> readMap =
                     ChronicleMapBuilder.of(Long.class, long[].class)
                             .averageValue(new long[150])
                             .createPersistedTo(file)) {
            long[] b = readMap.get(1L);
            assertArrayEquals(a, b, "long[] roundtrip");
        }
    }
}
