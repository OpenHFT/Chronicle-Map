/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.externalizable;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class ExternalizableTest {
    @Test
    public void externalizable() throws IOException {
        String path = OS.getTarget() + "/test-" + Time.uniqueId() + ".map";
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
