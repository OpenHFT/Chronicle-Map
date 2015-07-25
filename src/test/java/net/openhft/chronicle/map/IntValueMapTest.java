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

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.values.IntValue;
import net.openhft.lang.values.IntValue$$Native;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class IntValueMapTest {

    @Test
    public void test() throws IOException {

        try (final ChronicleMap<IntValue, CharSequence> map = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .entries(20000).create()) {
            IntValue$$Native value = new IntValue$$Native();
            value.bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);

            value.setValue(1);
            final String expected = "test";
            map.put(value, expected);

            final CharSequence actual = map.get(value);
            assertEquals(expected, actual);

            // this will fail

            map.toString();
        }
    }
}
