package net.openhft.chronicle.map;

import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;
import net.openhft.lang.values.IntValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DemoChronicleMapTest {

    @Test
    @Ignore
    public void testMap() throws Exception {
        File file = File.createTempFile("DummyOrders", ".test");
        file.deleteOnExit();
        int maxEntries = 1000;
        ChronicleMap<IntValue, DemoOrderVOInterface> map = ChronicleMapBuilder
                .of(IntValue.class, DemoOrderVOInterface.class)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .entries(maxEntries)
                .createPersistedTo(file);


        IntValue key = DataValueClasses.newInstance(IntValue.class);

        DemoOrderVOInterface value = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);
        DemoOrderVOInterface value2 = DataValueClasses.newDirectReference(DemoOrderVOInterface.class);

        // Initially populate the map
        for (int i = 0; i < maxEntries; i++) {
            key.setValue(i);

            map.acquireUsing(key, value);

            // TODO Shouldn't throw an NPE, because acquire should set the value.
            value.setSymbol("IBM-" + i);
            value.addAtomicOrderQty(1000);

            map.getUsing(key, value2);
            assertEquals("IBM-" + i, value.getSymbol());
            assertEquals(1000, value.getOrderQty(), 0.0);
        }

        for (Map.Entry<IntValue, DemoOrderVOInterface> entry : map.entrySet()) {

            IntValue k = entry.getKey();
            DemoOrderVOInterface v = entry.getValue();

            System.out.println(String.format("Key %d %s", k.getValue(), v == null ? "<null>" : v.getSymbol()));
            assertNotNull(v);
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
