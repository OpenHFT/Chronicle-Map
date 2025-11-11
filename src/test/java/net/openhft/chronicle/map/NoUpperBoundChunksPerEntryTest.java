/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

public class NoUpperBoundChunksPerEntryTest {

    @Test
    public void noUpperBoundChunksPerEntryTest() {
        ChronicleMap<Integer, CharSequence> map =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .averageValueSize(2).entries(10000L).actualSegments(1).create();
        String ultraLargeValue = "";
        for (int i = 0; i < 100; i++) {
            ultraLargeValue += "Hello";
        }
        map.put(1, ultraLargeValue);
        Assert.assertEquals(ultraLargeValue, map.get(1).toString());
    }
}
