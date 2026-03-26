/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class ChronicleMapEqualsTest {

    @Test
    void test() {
        ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .averageKey("a").averageValue("b")
                .entries(100)
                .create();

        HashMap<String, String> refMap = new HashMap<>();
        refMap.put("a", "b");
        map.putAll(refMap);
        assertTrue(map.equals(refMap));
    }
}
