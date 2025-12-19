/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NoUpperBoundChunksPerEntryTest {

    @Test
    public void noUpperBoundChunksPerEntryTest() {
        ChronicleMap<Integer, CharSequence> map =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .averageValueSize(2).entries(10000L).actualSegments(1).create();
        StringBuilder ultraLargeValue = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            ultraLargeValue.append("Hello");
        }
        map.put(1, ultraLargeValue.toString());
        Assertions.assertEquals(ultraLargeValue.toString(), map.get(1).toString(), "map.get(1).toString()");
    }
}
