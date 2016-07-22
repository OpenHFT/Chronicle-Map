/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

import org.junit.Assert;
import org.junit.Test;

public class Issue60Test {

    @Test
    public void issue60Test() {
        int entries = 200;
        int maxBloatFactor = 10;
        int loop = 1000;

        try (ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .entries(entries)
                .averageKeySize(12)
                .averageValueSize(12)
                .maxBloatFactor(maxBloatFactor)
                .create()) {

            System.out.println("begin test " + map.size());
            for (int i = 0; i < loop; i++) {
                map.put("key" + i, "value" + i);
            }
            System.out.println("map size " + map.size());

            int failedGet = 0;
            for (int i = 0; i < loop; i++) {
                String value = map.get("key" + i);
                if (value == null) {
                    failedGet++;
                }
            }
            Assert.assertEquals(0, failedGet);
            System.out.println("failedGet " + failedGet);
            System.out.println("map " + map);
        }
    }
}
