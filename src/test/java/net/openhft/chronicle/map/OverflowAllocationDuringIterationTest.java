/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
            }
        }
    }
}
