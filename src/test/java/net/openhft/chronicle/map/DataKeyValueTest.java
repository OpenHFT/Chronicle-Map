/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataKeyValueTest {

    @Test
    public void dataKeyValueTest() {
        ChronicleMap<IntValue, LongValue> map =
                ChronicleMapBuilder.of(IntValue.class, LongValue.class)
                        .entries(1000).create();
        IntValue heapKey = Values.newHeapInstance(IntValue.class);
        LongValue heapValue = Values.newHeapInstance(LongValue.class);
        final LongValue directValue = Values.newNativeReference(LongValue.class);

        heapKey.setValue(1);
        heapValue.setValue(1);
        map.put(heapKey, heapValue);
        assertEquals(1, map.get(heapKey).getValue());
        assertEquals(1, map.getUsing(heapKey, heapValue).getValue());

        heapKey.setValue(1);
        map.getUsing(heapKey, directValue).addValue(1);
        assertEquals(2, map.getUsing(heapKey, heapValue).getValue());

        heapKey.setValue(2);
        heapValue.setValue(3);
        map.put(heapKey, heapValue);
        assertEquals(3, map.get(heapKey).getValue());
    }
}
