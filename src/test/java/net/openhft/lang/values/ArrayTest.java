package net.openhft.lang.values;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.assertEquals;

public class ArrayTest {

    @Test
    public void test0() throws IOException {
        ClassAliasPool.CLASS_ALIASES.addAlias(MovingAverageArray.class);

        File file = new File(OS.getTarget() + "/pf-PosistionsAndClose-" + System.nanoTime());

        ChronicleMap<Long, MovingAverageArray> mapWrite = ChronicleMap
                .of(Long.class, MovingAverageArray.class)
                .entries(100)
                .averageValue(createSampleWithSize(6, 0))
                .createPersistedTo(file);
        mapWrite.put(1L, createSampleWithSize(6, 1));
        mapWrite.put(2L, createSampleWithSize(4, 2));
        mapWrite.close();

        ChronicleMap<Long, MovingAverageArray> mapRead = ChronicleMapBuilder
                .of(Long.class, MovingAverageArray.class)
                .createPersistedTo(file);
        MovingAverageArray m = mapRead.get(1L);
        assertEquals("!MovingAverageArray {\n" +
                "  values: [\n" +
                "    { movingAverage: 0.1, high: 0.1, low: 0.1, stdDev: 0.1 },\n" +
                "    { movingAverage: 1.1, high: 1.1, low: 1.1, stdDev: 1.1 },\n" +
                "    { movingAverage: 2.1, high: 2.1, low: 2.1, stdDev: 2.1 },\n" +
                "    { movingAverage: 3.1, high: 3.1, low: 3.1, stdDev: 3.1 },\n" +
                "    { movingAverage: 4.1, high: 4.1, low: 4.1, stdDev: 4.1 },\n" +
                "    { movingAverage: 5.1, high: 5.1, low: 5.1, stdDev: 5.1 }\n" +
                "  ]\n" +
                "}\n", m.toString());
        MovingAverageArray m2 = mapRead.getUsing(2L, m);
        assertSame(m, m2); // object is recycled, so no objects are created.
        assertEquals("!MovingAverageArray {\n" +
                "  values: [\n" +
                "    { movingAverage: 0.2, high: 0.2, low: 0.2, stdDev: 0.2 },\n" +
                "    { movingAverage: 1.2, high: 1.2, low: 1.2, stdDev: 1.2 },\n" +
                "    { movingAverage: 2.2, high: 2.2, low: 2.2, stdDev: 2.2 },\n" +
                "    { movingAverage: 3.2, high: 3.2, low: 3.2, stdDev: 3.2 }\n" +
                "  ]\n" +
                "}\n", m.toString());
    }

    private MovingAverageArray createSampleWithSize(int size, int seed) {
        MovingAverageArray sample = new MovingAverageArray();
        for (int i = 0; i < size; i++) {
            double value = i + seed / 10.0;
            sample.add(new MovingAverageCompact(value, value, value, value));
        }
        return sample;
    }
}