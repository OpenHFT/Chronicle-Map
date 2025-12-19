/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.impl.TypedMarshallableReaderWriter;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public final class AbstractMarshallableKeyValueTest {

    @Test
    public void shouldAcceptAbstractMarshallableComponents() {
        final ChronicleMap<Key, Value> map = ChronicleMapBuilder.of(Key.class, Value.class).entries(10).
                averageKey(new Key()).averageValue(new Value()).create();

        map.put(new Key(), new Value());

        assertThat("map.get(new Key()).number", map.get(new Key()).number, is(new Value().number));
    }

    @Test
    public void shouldAcceptAbstractMarshallableComponents2() {
        final ChronicleMap<Key, Marshallable> map = ChronicleMapBuilder.of(Key.class, Marshallable.class).entries(10)
                .averageKey(new Key()).averageValue(new Value())
                .valueMarshaller(new TypedMarshallableReaderWriter<>(Marshallable.class))
                .create();

        map.put(new Key(), new Value());

        Value value = (Value) map.get(new Key());
        assertThat("value.number", value.number, is(new Value().number));
    }

    private static final class Key extends SelfDescribingMarshallable {
        private final String k = "key";
    }

    private static final class Value extends SelfDescribingMarshallable {
        private final Integer number = 17;
    }
}
