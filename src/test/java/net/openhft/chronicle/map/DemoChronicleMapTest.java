/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
