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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class MaximumChronicleMapSizeTest {

    @Before
    public void assumeLinux() {
        Assume.assumeTrue(OS.isLinux());
    }

    @Test
    public void maximumChronicleMapSizeTest() {
        ChronicleMap<Long, Long> map = ChronicleMap
                .of(Long.class, Long.class)
                .entries(1000_000_000_000L)
                .create();
        map.put(1L, 1L);
    }
}
