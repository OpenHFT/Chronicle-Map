/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

public class NoUpperBoundChunksPerEntryTest {

    @Test
    public void noUpperBoundChunksPerEntryTest() {
        ChronicleMap<Integer, CharSequence> map =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .averageValueSize(1).entries(10000L).actualSegments(1).create();
        String ultraLargeValue = "";
        for (int i = 0; i < 100; i++) {
            ultraLargeValue += "Hello";
        }
        map.put(1, ultraLargeValue);
        Assert.assertEquals(ultraLargeValue, map.get(1));
    }
}
