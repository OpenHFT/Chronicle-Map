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

public class HColl423Test {

    @Test
    public void hColl423Test() {
        try {
            ChronicleMap<String, Integer> map = ChronicleMap
                    .of(String.class, Integer.class)
                    .averageKeySize(128)
                    .averageValueSize(100)
                    .entries(2_000_000L)
                    .maxBloatFactor(1_000.0)
                    .create();
            throw new AssertionError("should throw IllegalStateException");
        } catch (IllegalStateException expected) {
            // do nothing
        }
    }
}
