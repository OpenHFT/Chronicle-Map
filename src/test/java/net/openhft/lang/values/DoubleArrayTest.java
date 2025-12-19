/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.lang.values;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DoubleArrayTest {

    @Test
    public void testSetData() {
        DoubleArray da = new DoubleArray(64);
        da.setData(new double[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertEquals(8, da.length(), "da.length()");
        for (int i = 0; i < da.length(); i++)
            assertEquals(i + 1.0, da.getDataAt(i), 0.0, "da.getDataAt(i)");
        da.addData(9);
        da.addData(10);
        double[] ds = new double[64];
        int len = da.getDataUsing(ds);
        assertEquals(10, len, "len");
        for (int i = 0; i < len; i++)
            assertEquals(i + 1.0, ds[i], 0.0, "ds[i]");

        for (int i = 0; i < 64; i++)
            ds[i] = i * 1.01;
        da.setData(ds);
        assertEquals(64, da.length(), "da.length()");
        for (int i = 0; i < da.length(); i++)
            assertEquals(i * 1.01, da.getDataAt(i), 0.0, "da.getDataAt(i)");

        double[] ds2 = new double[65];
        assertEquals(64, da.getDataUsing(ds2), "da.getDataUsing(ds2)");
        for (int i = 0; i < 64; i++) {
            assertEquals(i * 1.01, da.getDataAt(i), 0.0, "da.getDataAt(i)");
            assertEquals(i * 1.01, ds2[i], 0.0, "ds2[i]");
        }

        assertThrows(IllegalArgumentException.class, () -> da.setData(ds2));
        // free the memory.
        da.bytesStore().releaseLast();
    }

    @Test
    public void addToAMap() {
        DoubleArray a = new DoubleArray(10);
        a.setData(new double[]{1, 2, 3, 4, 5});

        DoubleArray b = new DoubleArray(10);
        b.setData(new double[]{5, 6, 7, 8, 9});

        ChronicleMap<Integer, DoubleArray> proxyMap = ChronicleMap
                .of(Integer.class, DoubleArray.class)
                .constantValueSizeBySample(a)
                .entries(2)
                .create();
        proxyMap.put(1, a);
        proxyMap.put(2, b);

        DoubleArray storedA = proxyMap.get(1);
        assertNotNull(storedA, "stored DoubleArray should not be null after retrieval from map");
        assertEquals(a.length(), storedA.length(), "length roundtrip");
        assertEquals(a.getDataAt(0), storedA.getDataAt(0), 0.0, "first element roundtrip");

        System.out.println(proxyMap.get(1));
        System.out.println(proxyMap.get(2));
        proxyMap.close();
    }

    @Test
    @Disabled("TODO What is HACK???")
    public void addToAMap2() {
        DoubleArray.HACK = false;
        DoubleArray a = new DoubleArray(10);
        a.setData(new double[]{1, 2, 3, 4, 5});

        DoubleArray b = new DoubleArray(10);
        b.setData(new double[]{5, 6, 7, 8, 9});

        ChronicleMap<Integer, DoubleArray> proxyMap = ChronicleMapBuilder
                .of(Integer.class, DoubleArray.class)
                .averageValueSize(6 * 8)
                .create();
        proxyMap.put(1, a);
        proxyMap.put(2, b);

        assertNotNull(proxyMap.get(1), "stored DoubleArray should not be null when HACK mode is disabled");
        System.out.println(proxyMap.get(1));
        System.out.println(proxyMap.get(2));
        proxyMap.close();
        DoubleArray.HACK = true;
        assertTrue(DoubleArray.HACK, "DoubleArray.HACK restored");
    }
}
