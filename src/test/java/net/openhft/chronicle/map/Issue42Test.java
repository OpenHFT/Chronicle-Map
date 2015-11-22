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
                Assert.assertEquals(s, map.get(s));
            }
        }
    }
}
