package net.openhft.chronicle.map;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

public class Issue110Test {
    interface IContainer {
        @Array(length=10)
        double getDoubleArrayAt(int i);
        void setDoubleArrayAt(int i, double d);
    }

    @Test
    public void testChronicleDoubleArray() {
        ChronicleMap<String, IContainer> map =
                ChronicleMapBuilder.of(String.class, IContainer.class)
                        .entries(1024)
                        .averageKeySize(9)
                        .create();

        map.put("0", Values.newHeapInstance(IContainer.class));
        assert map.get("0") != null;
    }
}
