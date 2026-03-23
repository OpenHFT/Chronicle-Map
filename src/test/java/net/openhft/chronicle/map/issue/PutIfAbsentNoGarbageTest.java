/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.issue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

public class PutIfAbsentNoGarbageTest {

    static ChronicleMap<Long, LongValue> newShmLongLongValueUsing(int size, boolean putIfAbsentUsingValue) throws IOException {
        return ChronicleMapBuilder.simpleMapOf(Long.class, LongValue.class)
                .entries(size).putIfAbsentUsingValue(putIfAbsentUsingValue).create();
    }

    @Test
    public void testPutIfAbsentUsingValue() throws IOException, Throwable {
        try (ChronicleMap<Long, LongValue> map = newShmLongLongValueUsing(10, true)) {
            Long k = 1L;

            LongValue v1 = Values.newHeapInstance(LongValue.class);
            v1.setValue(1L);

            LongValue v2 = Values.newHeapInstance(LongValue.class);
            v2.setValue(1L);

            LongValue r = map.putIfAbsent(k, v1);
            assertNull(r);
            assertTrue(map.containsKey(k));

            LongValue s = map.putIfAbsent(k, v2);
            assertTrue(map.containsKey(k));
            assertEquals(s, v1);
            assertSame(v2, s, "should be same object");
        }
    }

    @Test
    public void testPutIfAbsentDefault() throws IOException, Throwable {
        try (ChronicleMap<Long, LongValue> map = newShmLongLongValueUsing(10, false)) {
            Long k = 1L;

            LongValue v1 = Values.newHeapInstance(LongValue.class);
            v1.setValue(1L);

            LongValue v2 = Values.newHeapInstance(LongValue.class);
            v2.setValue(1L);

            LongValue r = map.putIfAbsent(k, v1);
            assertNull(r);
            assertTrue(map.containsKey(k));

            LongValue s = map.putIfAbsent(k, v2);
            assertTrue(map.containsKey(k));
            assertEquals(s, v1);
            assertNotSame(v2, s, "should be same object");
        }
    }
}
