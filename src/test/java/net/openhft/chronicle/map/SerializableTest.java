/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.impl.CommonMarshallableReaderWriter;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

// From https://github.com/OpenHFT/Chronicle-Map/issues/183
@SuppressWarnings({"rawtypes", "unchecked", "serial"})
public class SerializableTest {
    @Test
    public void test1() {
        for (int i = 512; i < 514; i++) {
            System.out.println(i);
            try (ChronicleMap<Integer, Foo> map = ChronicleMapBuilder.of(Integer.class, Foo.class)
                    .name("foo")
                    .averageValueSize(32 + i * 2)
                    .entries(10)
                    .create()) {

                map.put(1, new Foo(i));
                assertNotNull(map.get(1), "Foo value should be retrievable after insertion");
                map.put(2, new Foo(i + 1));
                assertEquals(i + 2, map.get(2).x.length(), "Retrieved Foo string length should match expected size");
            }
        }
    }

    @Test
    public void test2() {
        ChronicleMap<Integer, Foo> map = ChronicleMapBuilder.of(Integer.class, Foo.class)
                .name("bar")
                .averageValueSize(4096)
                .entries(10)
                .create();

        String expected = expected();

        Foo value = new Foo(expected);
        map.put(1, value);
        String actual = map.get(1).x;

        assertEquals(expected, actual, "Serializable Foo string content should be preserved through serialization round-trip");
    }

    @Test
    public void test2b() {
        ChronicleMap<Integer, Bar> map = ChronicleMapBuilder.simpleMapOf(Integer.class, Bar.class)
                .name("bar")
                .averageValueSize(4096)
                .entries(10)
                .create();

        String expected = expected();

        Bar value = new Bar(expected);
        map.put(1, value);
        assertFalse(value.usesSelfDescribingMessage(), "BytesInBinaryMarshallable should not use self-describing message format");
        assertFalse(value.writeMarshallableWireOutCalled, "BytesInBinaryMarshallable writeMarshallable should not be called with simpleMapOf");
        String actual = map.get(1).x;

        assertEquals(expected, actual, "BytesInBinaryMarshallable string content should be preserved through simpleMapOf serialization round-trip");
    }

    @Test
    public void test2c() {
        ChronicleMap<Integer, Bar2> map = ChronicleMapBuilder.simpleMapOf(Integer.class, Bar2.class)
                .name("bar")
                .averageValueSize(1024)
                .entries(10)
                .create();

        String expected = expected();

        Bar2 value = new Bar2(expected);
        map.put(1, value);
        assertTrue(value.usesSelfDescribingMessage(), "SelfDescribingMarshallable should use self-describing message format");
        assertFalse(value.writeMarshallableWireOutCalled, "bytes marshallable called instead of writeMarshallable for simpleMapOf");
        String actual = map.get(1).x;

        assertEquals(expected, actual, "SelfDescribingMarshallable string content should be preserved through simpleMapOf serialization round-trip");
    }

    @Test
    public void test2d() {
        // if you create the Map of value type == Marshallable then it will use a TypedMarshallableReaderWriter
        ChronicleMap<Integer, Marshallable> map = ChronicleMapBuilder.simpleMapOf(Integer.class, Marshallable.class)
                .name("bar")
                .averageValueSize(1024)
                .entries(10)
                .create();

        String expected = expected();

        Bar2 value = new Bar2(expected);
        map.put(1, value);
        assertTrue(value.usesSelfDescribingMessage(), "SelfDescribingMarshallable should use self-describing format with Marshallable type");
        assertTrue(value.writeMarshallableWireOutCalled, "writeMarshallable should be called when using Marshallable type");
        Bar2 bar2 = (Bar2) map.get(1);
        String actual = bar2.x;

        assertEquals(expected, actual, "SelfDescribingMarshallable string content should be preserved through Marshallable type serialization round-trip");
    }

    @Test
    public void test2e() {
        ChronicleMap<Integer, Bar> map = ChronicleMapBuilder.simpleMapOf(Integer.class, Bar.class)
                .name("bar")
                .averageValueSize(4096)
                .entries(10)
                .valueMarshaller(new CommonMarshallableReaderWriter(Bar.class))
                .create();

        String expected = expected();

        Bar value = new Bar(expected);
        map.put(1, value);
        assertFalse(value.usesSelfDescribingMessage(), "BytesInBinaryMarshallable should not use self-describing with CommonMarshallableReaderWriter");
        assertFalse(value.writeMarshallableWireOutCalled, "BytesInBinaryMarshallable writeMarshallable not called with CommonMarshallableReaderWriter");
        String actual = map.get(1).x;

        assertEquals(expected, actual, "BytesInBinaryMarshallable string content should be preserved through CommonMarshallableReaderWriter serialization round-trip");
    }
    @Test
    public void test2f() {
        ChronicleMap<Integer, Bar2> map = ChronicleMapBuilder.simpleMapOf(Integer.class, Bar2.class)
                .name("bar")
                .averageValueSize(1024)
                .entries(10)
                .valueMarshaller(new CommonMarshallableReaderWriter(Bar2.class))
                .create();

        String expected = expected();

        Bar2 value = new Bar2(expected);
        map.put(1, value);
        assertTrue(value.usesSelfDescribingMessage(), "SelfDescribingMarshallable should use self-describing with CommonMarshallableReaderWriter");
        assertTrue(value.writeMarshallableWireOutCalled, "writeMarshallable should be called with CommonMarshallableReaderWriter");
        String actual = map.get(1).x;

        assertEquals(expected, actual, "SelfDescribingMarshallable string content should be preserved through CommonMarshallableReaderWriter serialization round-trip");
    }

    @NotNull
    private static String expected() {
        return IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));
    }

    public static class Foo implements Serializable {
        public final String x;

        Foo(int length) {
            this.x = "x" + IntStream.range(0, length)
                    .mapToObj(i -> "ä")
                    .collect(Collectors.joining(""));
        }

        public Foo(String expected) {
            x = expected;
        }
    }

    static class Bar extends BytesInBinaryMarshallable {

        final String x;
        transient boolean writeMarshallableWireOutCalled;

        public Bar(String expected) {
            this.x = expected;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            this.writeMarshallableWireOutCalled = true;
            super.writeMarshallable(wire);
        }

        @Override
        public String toString() {
            return "Bar{" +
                    "x='" + x + '\'' +
                    ", writeMarshallableWireOutCalled=" + writeMarshallableWireOutCalled +
                    '}';
        }
    }

    static class Bar2 extends SelfDescribingMarshallable {

        final String x;
        transient boolean writeMarshallableWireOutCalled;

        public Bar2(String expected) {
            this.x = expected;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            this.writeMarshallableWireOutCalled = true;
            super.writeMarshallable(wire);
        }

        @Override
        public String toString() {
            return "Bar2{" +
                    "x='" + x + '\'' +
                    ", writeMarshallableWireOutCalled=" + writeMarshallableWireOutCalled +
                    '}';
        }
    }
}
