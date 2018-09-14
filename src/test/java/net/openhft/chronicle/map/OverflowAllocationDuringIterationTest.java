/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import org.junit.Test;

public class OverflowAllocationDuringIterationTest {

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
                    ((VanillaChronicleMap) map).verifyTierCountersAreaData();
                }
            }
        }
    }
}
