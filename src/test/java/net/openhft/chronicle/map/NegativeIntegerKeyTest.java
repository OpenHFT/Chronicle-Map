/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class NegativeIntegerKeyTest {

    @Test
    void testNegativeIntegerKey() throws IOException {
        File file = ChronicleMapTest.getPersistenceFile();
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .createPersistedTo(file)) {
            map.put(-1, -1);
        }
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .recoverPersistedTo(file, true)) {
            assertEquals(Integer.valueOf(-1), map.get(-1));
        }
    }
}
