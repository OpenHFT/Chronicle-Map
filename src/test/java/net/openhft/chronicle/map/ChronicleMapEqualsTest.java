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

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class ChronicleMapEqualsTest {

    @Test
    public void test() {
        ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .averageKey("a").averageValue("b")
                .entries(100)
                .create();

        HashMap<String, String> refMap = new HashMap<>();
        refMap.put("a", "b");
        map.putAll(refMap);
        assertTrue(map.equals(refMap));
    }
}
