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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
