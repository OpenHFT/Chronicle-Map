package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Rob Austin.
 */


@RunWith(Parameterized.class)
public class MapFullTest {

    private final int segments;
    private final long maxEntries;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Object[][]{
                {
                        1, 1
                },
                {
                        8, 1
                },
                {
                        8, 8
                },
                {
                        8, 1024
                }
        });
    }


    public MapFullTest(int segments, long maxEntries) {
        this.segments = segments;
        this.maxEntries = maxEntries;
    }

    @Test
    public void testThatWeCanPutALotOfEntriesInAMap() throws Exception {

        char[] c = new char[2 << 20];
        Arrays.fill(c, 'X');
        final String value = new String(c);

        final ChronicleMapBuilder chronicleMapBuilder = new ChronicleMapBuilder(Integer.class,
                CharSequence.class);
        final long size = maxEntries;
        chronicleMapBuilder.entries(maxEntries);
        chronicleMapBuilder.replication((byte) 1);
        chronicleMapBuilder.actualSegments(segments);
        chronicleMapBuilder.averageValueSize(value.length());

        final ChronicleMap chronicleMap = chronicleMapBuilder.create();

        for (int i = 0; i < size; i++) {
            chronicleMap.put(i, value);
        }

        Assert.assertEquals(size, chronicleMap.size());


        chronicleMap.clear();

        for (int i = 0; i < size; i++) {
            chronicleMap.put(i, value);
        }

        Assert.assertEquals(size, chronicleMap.size());

        chronicleMap.remove(0);
        Assert.assertEquals(size - 1, chronicleMap.size());

    }
}
