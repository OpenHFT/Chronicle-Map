/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.set.ChronicleSet;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ChronicleMapNameTest {

    @Test
    public void testChronicleMapName() {
        ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .name("foo")
                .create();
        assertTrue(map.toIdentityString().contains("foo"));
    }

    @Test
    public void testChronicleSetName() {
        ChronicleSet<Integer> set = ChronicleSet
                .of(Integer.class)
                .entries(1)
                .name("foo")
                .create();
        assertTrue(set.toIdentityString().contains("foo"));
    }
}
