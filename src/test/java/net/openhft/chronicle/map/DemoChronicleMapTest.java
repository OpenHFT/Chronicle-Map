/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static net.openhft.chronicle.values.Values.newNativeReference;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

interface DemoOrderVOInterface {
    public CharSequence getSymbol();
//    public StringBuilder getUsingSymbol(StringBuilder sb);

    public void setSymbol(@MaxUtf8Length(20) CharSequence symbol);

    public double addAtomicOrderQty(double toAdd);

    public double getOrderQty();

    public void setOrderQty(double orderQty);

}

public class DemoChronicleMapTest {

    @Test
    public void testMap() throws IOException {
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
    public void testMapLocked() throws IOException {
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
}
