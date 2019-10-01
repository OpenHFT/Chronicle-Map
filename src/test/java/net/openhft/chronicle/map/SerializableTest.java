package net.openhft.chronicle.map;

import net.openhft.chronicle.wire.AbstractBytesMarshallable;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.junit.Test;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

// From https://github.com/OpenHFT/Chronicle-Map/issues/183
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

                System.out.println("put 1");
                map.put(1, new Foo(i));
                System.out.println("get 1");
                map.get(1);
                System.out.println("works");
                System.out.println("put 2");
                map.put(2, new Foo(i + 1));
                System.out.println("get 2");
                assertEquals(i + 2,
                        map.get(2).x.length());
                System.out.println("doesn't work");
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

        String expected = IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));

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

        String expected = IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));

        Bar value = new Bar(expected);
        map.put(1, value);
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

        String expected = IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));

        Bar2 value = new Bar2(expected);
        map.put(1, value);
        String actual = map.get(1).x;

        assertEquals(expected, actual);
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

    static class Bar extends AbstractBytesMarshallable {

        final String x;

        public Bar(String expected) {
            this.x = expected;
        }
    }

    static class Bar2 extends AbstractMarshallable {

        final String x;

        public Bar2(String expected) {
            this.x = expected;
        }
    }
}
