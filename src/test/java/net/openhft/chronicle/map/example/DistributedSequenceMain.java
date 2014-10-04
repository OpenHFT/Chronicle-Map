package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.io.File;
import java.io.IOException;

public class DistributedSequenceMain {

    public static void main(String... ignored) throws IOException {
        Class<LongValue> longValueClass = DataValueClasses.directClassFor(LongValue.class);
        ChronicleMap<String, LongValue> map =
                ChronicleMapBuilder.of(String.class, longValueClass)
                        .entries(128)
                        .actualSegments(1)
                        .create(new File("/tmp/counters"));
        LongValue value = DataValueClasses.newDirectReference(longValueClass);
        map.acquireUsing("sequence", value);

        for (int i = 0; i < 1000000; i++) {
            long nextId = value.addAtomicValue(1);
        }

        map.close();
    }
}
