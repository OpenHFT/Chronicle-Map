/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class Issue42Test {

    @Test
    public void crashJVMWindowsTest() throws IOException {

        if (!OS.isWindows())
            return;

        try (final ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .averageKeySize(5.9).averageValueSize(5.9)
                .entries(1000000)
                .minSegments(128).create()) {
            for (int i = 0; i < 1000000; ++i) {
                String s = String.valueOf(i);
                map.put(s, s);
            }

            for (int i = 0; i < 1000000; ++i) {
                String s = String.valueOf(i);
                Assert.assertEquals(s, map.get(s).toString());
            }
        }
    }
}
