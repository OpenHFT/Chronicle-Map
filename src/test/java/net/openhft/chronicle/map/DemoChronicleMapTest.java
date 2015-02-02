package net.openhft.chronicle.map;

import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;
import net.openhft.lang.values.IntValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

public class DemoChronicleMapTest {

    @Test
    public void testMap() throws Exception {
        File file = File.createTempFile("DummyOrders"+System.currentTimeMillis(), ".test");
        file.deleteOnExit();
        int maxEntries = 1000;
        try (ChronicleMap<IntValue, DemoOrderVOInterface> map = ChronicleMapBuilder
                .of(IntValue.class, DemoOrderVOInterface.class)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .entries(maxEntries)
                .createPersistedTo(file)) {
            IntValue key = DataValueClasses.newDirectInstance(IntValue.class);

            DemoOrderVOInterface value = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);
            DemoOrderVOInterface value2 = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);

            // Initially populate the map
            for (int i = 0; i < maxEntries; i++) {
                key.setValue(i);

                map.acquireUsing(key, value);

                value.setSymbol("IBM-" + i);
                value.addAtomicOrderQty(1000);

                map.getUsing(key, value2);
                assertEquals("IBM-" + i, value.getSymbol());
                assertEquals(1000, value.getOrderQty(), 0.0);
            }

            for (Map.Entry<IntValue, DemoOrderVOInterface> entry : map.entrySet()) {
                IntValue k = entry.getKey();
                DemoOrderVOInterface v = entry.getValue();

//                System.out.println(String.format("Key %d %s", k.getValue(), v == null ? "<null>" : v.getSymbol()));
                assertNotNull(v);
            }
        }
    }

    @Test
    public void testMapLocked() throws Exception {
        File file = File.createTempFile("DummyOrders-"+System.currentTimeMillis(), ".test");
        file.deleteOnExit();
        int maxEntries = 1000;
        try (ChronicleMap<IntValue, DemoOrderVOInterface> map = ChronicleMapBuilder
                .of(IntValue.class, DemoOrderVOInterface.class)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .entries(maxEntries)
                .createPersistedTo(file)) {
            IntValue key = DataValueClasses.newDirectInstance(IntValue.class);

            DemoOrderVOInterface value = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);
            DemoOrderVOInterface value2 = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);

            // Initially populate the map
            for (int i = 0; i < maxEntries; i++) {
                key.setValue(i);

                try (WriteContext wc = map.acquireUsingLocked(key, value)) {
                    value.setSymbol("IBM-" + i);
                    value.addAtomicOrderQty(1000);
                }

                try (ReadContext rc = map.getUsingLocked(key, value2)) {
                    assertTrue(rc.present());
                    assertEquals("IBM-" + i, value.getSymbol());
                    assertEquals(1000, value.getOrderQty(), 0.0);
                }
            }

            for (Map.Entry<IntValue, DemoOrderVOInterface> entry : map.entrySet()) {
                IntValue k = entry.getKey();
                DemoOrderVOInterface v = entry.getValue();

//                System.out.println(String.format("Key %d %s", k.getValue(), v == null ? "<null>" : v.getSymbol()));
                assertNotNull(v);
            }
        }
    }
}

interface DemoOrderVOInterface {
    public CharSequence getSymbol();
//    public StringBuilder getUsingSymbol(StringBuilder sb);

    public void setSymbol(@MaxSize(20) CharSequence symbol);

    public double addAtomicOrderQty(double toAdd);

    public double getOrderQty();

    public void setOrderQty(double orderQty);

}
