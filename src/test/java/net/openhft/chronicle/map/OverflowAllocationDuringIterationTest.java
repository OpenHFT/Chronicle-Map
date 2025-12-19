/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class OverflowAllocationDuringIterationTest {

    @Disabled("https://teamcity.chronicle.software/viewLog.html?buildId=639359&buildTypeId=Chronicle_ChronicleMap_SnapshotARM")
    @Test
    public void testOverflowAllocationDuringIteration() {
        int entries = 10000;
        String x = "x";
        try (ChronicleMap<Integer, CharSequence> map = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .averageValue(x)
                .entries(entries)
                .maxBloatFactor(10.0)
                .actualSegments(1)
                .create()) {
            for (int i = 0; i < entries; i++) {
                map.put(i, x);
            }
            for (int i = 0; i < 3; i++) {
                try {
                    final String currentX = x;
                    map.forEachEntry(e -> {
                        String v = e.value().get().toString();
                        if (!currentX.contentEquals(v)) {
                            throw new AssertionError(currentX + " != " + v + "<=" + e.key());
                        }
                        e.doReplaceValue(e.context().wrapValueAsData(v + v));
                    });
                    x = x + x;
                    for (int j = 0; j < entries; j++) {
                        if (map.get(j) == null || !x.contentEquals(map.get(j))) {
                            throw new AssertionError();
                        }
                    }
                } finally {
                    ((VanillaChronicleMap<?, ?, ?>) map).verifyTierCountersAreaData();
                }
            }
        }
    }
}
