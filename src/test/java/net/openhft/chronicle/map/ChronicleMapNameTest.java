/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.set.ChronicleSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChronicleMapNameTest {

    @Test
    public void testChronicleMapName() {
        ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .name("foo")
                .create();
        Assertions.assertTrue(map.toIdentityString().contains("foo"), "map.toIdentityString().contains(<str>)");
    }

    @Test
    public void testChronicleSetName() {
        ChronicleSet<Integer> set = ChronicleSet
                .of(Integer.class)
                .entries(1)
                .name("foo")
                .create();
        Assertions.assertTrue(set.toIdentityString().contains("foo"), "set.toIdentityString().contains(<str>)");
    }
}
