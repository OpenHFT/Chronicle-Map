/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class KeySizesTest {
    @Test
    public void testDifferentKeySizes() throws IOException {

        Map<String, String> map = ChronicleMap.of(String.class, String.class)
                .entries(100).averageKeySize(100).averageValueSize(100).create();

        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            String k = keyBuilder.toString();
            map.put(k, k);
            String k2 = map.get(k);
            assertEquals(k, k2, "retrieved value should match the key stored in the map");
            keyBuilder.append('a');
        }
        keyBuilder.setLength(0);
        for (int i = 0; i < 100; i++) {
            String k = keyBuilder.toString();
            String k2 = map.get(k);
            assertEquals(k, k2, "retrieved value should match the key for all stored keys");
            keyBuilder.append('a');
        }

        ((Closeable) map).close();
    }
}
