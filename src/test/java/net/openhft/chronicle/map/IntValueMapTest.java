/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class IntValueMapTest {

    @Test
    public void test() throws IOException {

        try (final ChronicleMap<IntValue, CharSequence> map = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .averageValue("test")
                .entries(20000).create()) {
            IntValue value = Values.newNativeReference(IntValue.class);
            ((Byteable) value).bytesStore(NativeBytesStore.nativeStoreWithFixedCapacity(4), 0, 4);

            value.setValue(1);
            final String expected = "test";
            map.put(value, expected);

            final CharSequence actual = map.get(value);
            assertEquals(expected, actual.toString());

            // this will fail

            map.toString();
        }
    }
}
