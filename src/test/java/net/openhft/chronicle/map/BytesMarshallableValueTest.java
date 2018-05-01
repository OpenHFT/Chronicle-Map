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

import net.openhft.chronicle.bytes.BytesMarshallable;
import org.junit.Assert;
import org.junit.Test;

public class BytesMarshallableValueTest {

    @Test
    public void bytesMarshallableValueTest() {
        try (ChronicleMap<Integer, Value> map = ChronicleMap
                .of(Integer.class, Value.class)
                .averageValue(new Value(1, "foo"))
                .entries(10)
                .create()) {
            map.put(1, new Value(1, "bar"));
            Assert.assertEquals("bar", map.replace(1, new Value(2, "baz")).foo);
            map.remove(1);
        }
    }

    public static class Value implements BytesMarshallable {
        int x;
        String foo;

        public Value(int x, String foo) {
            this.x = x;
            this.foo = foo;
        }
    }
}
