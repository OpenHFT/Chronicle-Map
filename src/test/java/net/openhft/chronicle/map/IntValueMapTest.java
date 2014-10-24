/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

        final ChronicleMap<IntValue, CharSequence> map = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .entries(20000)
                .keyMarshaller(ByteableIntValueMarshaller.INSTANCE).create();

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
