/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
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
            ((Byteable) value).bytesStore(BytesStore.nativeStoreWithFixedCapacity(4), 0, 4);

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
