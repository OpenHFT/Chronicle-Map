/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import org.junit.Test;

import java.util.Random;

public class Issue3Test {

    @Test
    public void test() {
        try (ChronicleSet<Long> set = ChronicleSetBuilder.of(Long.class)
                .actualSegments(1)
                .entriesPerSegment(1000)
                .create()) {
            Random r = new Random();
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 300; j++) {
                    set.add(r.nextLong());
                }
                set.clear();
            }
        }
    }
}
