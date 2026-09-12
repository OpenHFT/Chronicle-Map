/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static net.openhft.chronicle.values.Values.newNativeReference;
import static org.junit.jupiter.api.Assertions.*;

interface DemoOrderVOInterface {
    CharSequence getSymbol();
    //    public StringBuilder getUsingSymbol(StringBuilder sb);

    void setSymbol(@MaxUtf8Length(20) CharSequence symbol);

    double addAtomicOrderQty(double toAdd);

    double getOrderQty();

    void setOrderQty(double orderQty);

}

class DemoChronicleMapTest {

    @Test
    void testMap() throws IOException {
        File file = File.createTempFile("DummyOrders" + System.currentTimeMillis(), ".test");
        file.deleteOnExit();
        int maxEntries = 1000;
        try (ChronicleMap<IntValue, DemoOrderVOInterface> map = ChronicleMapBuilder
                .of(IntValue.class, DemoOrderVOInterface.class)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .entries(maxEntries)
                .entryAndValueOffsetAlignment(8)
                .createPersistedTo(file)) {
            IntValue key = Values.newHeapInstance(IntValue.class);

            DemoOrderVOInterface value = newNativeReference(DemoOrderVOInterface.class);
            DemoOrderVOInterface value2 = newNativeReference(DemoOrderVOInterface.class);

            // Initially populate the map
            for (int i = 0; i < maxEntries; i++) {
                key.setValue(i);

                map.acquireUsing(key, value);

                value.setSymbol("IBM-" + i);
                value.addAtomicOrderQty(1000);

                map.getUsing(key, value2);
                assertEquals("IBM-" + i, value.getSymbol().toString());
                assertEquals(1000, value.getOrderQty(), 0.0);
            }

            for (Map.Entry<IntValue, DemoOrderVOInterface> entry : map.entrySet()) {
                IntValue k = entry.getKey();
                DemoOrderVOInterface v = entry.getValue();

                //                System.out.println(String.format("Key %d %s", k.getValue(), v == null ? "<null>" : v.getSymbol()));
                assertNotNull(v);
            }
        }

        file.delete();
    }

    @Test
    void testMapLocked() throws IOException {
        File file = File.createTempFile("DummyOrders-" + System.currentTimeMillis(), ".test");
        file.deleteOnExit();
        int maxEntries = 1000;
        try (ChronicleMap<IntValue, DemoOrderVOInterface> map = ChronicleMapBuilder
                .of(IntValue.class, DemoOrderVOInterface.class)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .entries(maxEntries)
                .entryAndValueOffsetAlignment(8)
                .createPersistedTo(file)) {
            IntValue key = Values.newHeapInstance(IntValue.class);

            DemoOrderVOInterface value = newNativeReference(DemoOrderVOInterface.class);
            DemoOrderVOInterface value2 = newNativeReference(DemoOrderVOInterface.class);

            // Initially populate the map
            for (int i = 0; i < maxEntries; i++) {
                key.setValue(i);

                try (net.openhft.chronicle.core.io.Closeable c =
                             map.acquireContext(key, value)) {
                    assertNotNull(c);
                    value.setSymbol("IBM-" + i);
                    value.addAtomicOrderQty(1000);
                }

                // TODO suspicious -- getUsing `value2`, working with `value` then
                //                try (ReadContext rc = map.getUsingLocked(key, value2)) {
                //                    assertTrue(rc.present());
                //                    assertEquals("IBM-" + i, value.getSymbol());
                //                    assertEquals(1000, value.getOrderQty(), 0.0);
                //                }
            }

            for (Map.Entry<IntValue, DemoOrderVOInterface> entry : map.entrySet()) {
                IntValue k = entry.getKey();
                DemoOrderVOInterface v = entry.getValue();

                //                System.out.println(String.format("Key %d %s", k.getValue(), v == null ? "<null>" : v.getSymbol()));
                assertNotNull(v);
            }
        }
        file.delete();
    }

    @Test
    void testNegativeIllegalAlignment() {
        assertThrows(IllegalArgumentException.class, () -> ChronicleMapBuilder.of(IntValue.class, DemoOrderVOInterface.class).entryAndValueOffsetAlignment(-1));
    }

    @Test
    void testNotPowerOfTwoIllegalAlignment() {
        assertThrows(IllegalArgumentException.class, () -> ChronicleMapBuilder.of(IntValue.class, DemoOrderVOInterface.class).entryAndValueOffsetAlignment(13));
    }
}
