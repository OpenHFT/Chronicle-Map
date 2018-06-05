package net.openhft.chronicle.map.externalizable;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ExternalizableTest {
    @Test
    public void externalizable() throws IOException {
        String path = OS.TARGET + "/test-" + System.nanoTime() + ".map";
        new File(path).deleteOnExit();
        try (ChronicleMap<Long, SomeClass> storage = ChronicleMapBuilder
                .of(Long.class, SomeClass.class)
                .averageValueSize(128)
                .entries(128)
                .createPersistedTo(new File(path))) {
            SomeClass value = new SomeClass();
            value.hits.add("one");
            value.hits.add("two");
            storage.put(1L, value);

            SomeClass value2 = storage.get(1L);
            assertEquals(value.hits, value2.hits);
        }
    }
}
