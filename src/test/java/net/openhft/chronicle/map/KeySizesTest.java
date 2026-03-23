/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class KeySizesTest {
    @Test
    public void testDifferentKeySizes() throws IOException {

        Map<String, String> map = ChronicleMap.of(String.class, String.class)
                .entries(100).averageKeySize(100).averageValueSize(100).create();

        String k = "";
        for (int i = 0; i < 100; i++) {
            map.put(k, k);
            String k2 = map.get(k);
            assertEquals(k, k2);
            k += "a";
        }
        k = "";
        for (int i = 0; i < 100; i++) {
            String k2 = map.get(k);
            assertEquals(k, k2);
            k += "a";
        }

        ((Closeable) map).close();
    }
}
