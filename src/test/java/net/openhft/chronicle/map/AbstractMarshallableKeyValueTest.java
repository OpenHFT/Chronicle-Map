/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.impl.TypedMarshallableReaderWriter;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class AbstractMarshallableKeyValueTest {

    @Test
    void shouldAcceptAbstractMarshallableComponents() throws Exception {
        final ChronicleMap<Key, Value> map = ChronicleMapBuilder.of(Key.class, Value.class).entries(10).
                averageKey(new Key()).averageValue(new Value()).create();

        map.put(new Key(), new Value());

        assertEquals(new Value().number, map.get(new Key()).number);
    }

    @Test
    void shouldAcceptAbstractMarshallableComponents2() throws Exception {
        final ChronicleMap<Key, Marshallable> map = ChronicleMapBuilder.of(Key.class, Marshallable.class).entries(10)
                .averageKey(new Key()).averageValue(new Value())
                .valueMarshaller(new TypedMarshallableReaderWriter<>(Marshallable.class))
                .create();

        map.put(new Key(), new Value());

        Value value = (Value) map.get(new Key());
        assertEquals(new Value().number, value.number);
    }

    private static final class Key extends SelfDescribingMarshallable {
        private String k = "key";
    }

    private static final class Value extends SelfDescribingMarshallable {
        private Integer number = 17;
    }
}
