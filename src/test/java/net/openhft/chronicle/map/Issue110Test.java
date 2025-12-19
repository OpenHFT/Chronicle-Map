/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Issue110Test {
    @Test
    public void testChronicleDoubleArray() {
        try (ChronicleMap<String, IContainer> map =
                     ChronicleMapBuilder.of(String.class, IContainer.class)
                             .entries(1024)
                             .averageKeySize(9)
                             .create()) {

            map.put("0", Values.newHeapInstance(IContainer.class));
            assertNotNull(map.get("0"), "map.get should return value");
        }
    }

    interface IContainer {
        @Array(length = 10)
        double getDoubleArrayAt(int i);

        void setDoubleArrayAt(int i, double d);
    }
}
