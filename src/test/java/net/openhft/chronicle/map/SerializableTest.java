package net.openhft.chronicle.map;

import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Ignore("TODO Fix https://github.com/OpenHFT/Chronicle-Map/issues/183")
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
                .averageValueSize(1024)
                .entries(10)
                .create();

        String expected = IntStream.range(0, 1100)
                .mapToObj(i -> i % 50 == 49 ? "\n" : "" + i % 10)
                .collect(Collectors.joining(""));


        map.put(1, new Foo(expected));
        String actual = map.get(1).x;

        assertEquals(expected, actual);
    }

    static class Foo implements Serializable {
        String x;

        Foo(int length) {
            this.x = "x" + IntStream.range(0, length)
                    .mapToObj(i -> "ä")
                    .collect(Collectors.joining(""));
        }

        Foo(String expected) {
            x = expected;
        }
    }
}
