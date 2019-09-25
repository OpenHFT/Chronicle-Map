package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.map.SerializableTest;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SerializableDataAccessTest {

    @Test
    public void testSerializableDataAccess() {
        SerializableDataAccess<SerializableTest.Foo> sda = new SerializableDataAccess<>();
        sda.initTransients(8192);

        String expected = IntStream.range(0, 4096)
                .mapToObj(i -> i % 50 == 0 ? String.format("\n%04d", i) : "" + i % 10)
                .collect(Collectors.joining(""));

        SerializableTest.Foo value = new SerializableTest.Foo(expected);
        sda.getData(value);
        SerializableTest.Foo foo = sda.getUsing(null);
        assertEquals(expected, foo.x);
    }

}