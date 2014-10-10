package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class ChronicleSetBuilderTest {

    @Test
    public void test() throws Exception {

        Set<Integer> integers = ChronicleSetBuilder.of(Integer.class).create();

        for (int i = 0; i < 10; i++) {
            integers.add(i);
        }

        Assert.assertTrue(integers.contains(5));
        Assert.assertEquals(10, integers.size());
    }


}
