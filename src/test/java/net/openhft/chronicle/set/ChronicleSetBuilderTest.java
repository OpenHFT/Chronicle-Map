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

package net.openhft.chronicle.set;

import org.junit.Assert;
import org.junit.Test;

public class ChronicleSetBuilderTest {

    @Test
    public void test() {

        try (ChronicleSet<Integer> integers = ChronicleSetBuilder.of(Integer.class).create()) {
            for (int i = 0; i < 10; i++) {
                integers.add(i);
            }

            Assert.assertTrue(integers.contains(5));
            Assert.assertEquals(10, integers.size());
        }
    }
}
