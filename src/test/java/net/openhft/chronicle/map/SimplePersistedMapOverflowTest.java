/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.set.Builder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimplePersistedMapOverflowTest {

    @Test
    public void simplePersistedMapOverflowTest() throws IOException {
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1_000)
                .createPersistedTo(Builder.getPersistenceFile())) {
            for (int i = 0; i < 2_000; i++) {
                map.put(i, i);
            }
            assertEquals(2_000, map.size(), "map size after insert");
        }
    }
}
