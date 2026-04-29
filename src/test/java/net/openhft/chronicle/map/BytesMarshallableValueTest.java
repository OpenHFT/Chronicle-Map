/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
        final int x;
        final String foo;

        public Value(int x, String foo) {
            this.x = x;
            this.foo = foo;
        }
    }
}
