/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class BooleanValuesTest {

    /**
     * see issue <a href="http://stackoverflow.com/questions/26219313/strange-npe-from-chronicle-map-toy-code">here</a>
     */
    @Test
    public void testTestBooleanValues() {
        try (ChronicleMap<Integer, Boolean> map = ChronicleMap.of(Integer.class, Boolean.class)
                .entries(1).create()) {
            map.put(7, true);
            Assert.assertEquals(true, map.get(7));
        }
    }
}
