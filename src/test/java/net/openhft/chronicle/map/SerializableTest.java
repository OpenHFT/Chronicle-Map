package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.impl.CommonMarshallableReaderWriter;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

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
                map.get(1);
                map.put(2, new Foo(i + 1));
                assertEquals(i + 2, map.get(2).x.length());
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

        assertEquals(expected, actual);
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
        assertFalse(value.usesSelfDescribingMessage());
        assertFalse(value.writeMarshallableWireOutCalled);
        String actual = map.get(1).x;

        assertEquals(expected, actual);
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
        assertTrue(value.usesSelfDescribingMessage());
        assertFalse("we call bytes marshallable in this case", value.writeMarshallableWireOutCalled);
        String actual = map.get(1).x;

        assertEquals(expected, actual);
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
        assertTrue(value.usesSelfDescribingMessage());
        assertTrue(value.writeMarshallableWireOutCalled);
        Bar2 bar2 = (Bar2) map.get(1);
        String actual = bar2.x;

        assertEquals(expected, actual);
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
        assertFalse(value.usesSelfDescribingMessage());
        assertFalse(value.writeMarshallableWireOutCalled);
        String actual = map.get(1).x;

        assertEquals(expected, actual);
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
        assertTrue(value.usesSelfDescribingMessage());
        assertTrue(value.writeMarshallableWireOutCalled);
        String actual = map.get(1).x;

        assertEquals(expected, actual);
    }

    @NotNull
    private static String expected() {
        String expected = IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));
        return expected;
    }

    public static class Foo implements Serializable {
        public String x;

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
