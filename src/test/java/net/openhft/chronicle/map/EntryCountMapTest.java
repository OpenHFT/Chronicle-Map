package net.openhft.chronicle.map;

import net.openhft.lang.values.LongValue;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static net.openhft.chronicle.map.Alignment.OF_4_BYTES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EntryCountMapTest {
    @Test
    public void testVerySmall() throws Exception {
        int s = 1, i = 1;
        // regression test.
        testEntriesMaxSize(s, 1, 64, i);
        testEntriesMaxSize(s, 2, 64, i);
        testEntriesMaxSize(s, 4, 64, i);
        testEntriesMaxSize(s, 8, 64, i);
        testEntriesMaxSize(s, 16, 64, i);
        testEntriesMaxSize(s, 32, 64, i);
        testEntriesMaxSize(s, 64, 128, i);
    }

    @Test
    public void testSmall() throws Exception {
        for (int i = 0; i < 10; i++) {
            // regression test.
            for (int s : new int[]{1, 2, 4, 8}) {
                if (s <= 4)
                    testEntriesMaxSize(s, 128, 256, i);
                testEntriesMaxSize(s, 256, 510, i);
                testEntriesMaxSize(s, 512, 1010, i);
                testEntriesMaxSize(s, 1000, 1730, i);
                testEntriesMaxSize(s, 2000, 2800, i);
                testEntriesMaxSize(s, 4000, 5100, i);
                testEntriesMaxSize(s, 5000, 6800, i);
                testEntriesMaxSize(s, 8000, 9800, i);
                testEntriesMaxSize(s, 12000, 15000, i);
                testEntriesMaxSize(s, 16000, 20000, i);
            }
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    @Test
    public void testMedium() throws Exception {
        for (int i = 0; i < 3; i++) {
            // regression test.
            int s = 16;
            testEntriesMaxSize(s, 512, 1000, i);
            testEntriesMaxSize(s, 1000, 2100, i);
            testEntriesMaxSize(s, 2000, 3100, i);
            testEntriesMaxSize(s, 4000, 5100, i);
            testEntriesMaxSize(s, 5000, 7100, i);
            testEntriesMaxSize(s, 8000, 10100, i);
            testEntriesMaxSize(s, 12000, 15100, i);
            testEntriesMaxSize(s, 16000, 20200, i);
            s = 32;
            testEntriesMaxSize(s, 2000, 3900, i);
            testEntriesMaxSize(s, 4000, 5900, i);
            testEntriesMaxSize(s, 5000, 7900, i);
            testEntriesMaxSize(s, 8000, 12000, i);
            testEntriesMaxSize(s, 12000, 15900, i);
            testEntriesMaxSize(s, 16000, 20100, i);
            testEntriesMaxSize(s, 32000, 41000, i);
            s = 64;
            testEntriesMaxSize(s, 5000, 7550, i);
            testEntriesMaxSize(s, 8000, 11400, i);
            testEntriesMaxSize(s, 12000, 15400, i);
            testEntriesMaxSize(s, 16000, 20000, i);
            testEntriesMaxSize(s, 32000, 39400, i);
            testEntriesMaxSize(s, 40000, 52200, i);
            s = 128;
            testEntriesMaxSize(s, 16000, 23000, i);
            testEntriesMaxSize(s, 32000, 39000, i);
            testEntriesMaxSize(s, 40000, 55200, i);
            testEntriesMaxSize(s, 64000, 79800, i);
            testEntriesMaxSize(s, 129000, 160000, i);
            s = 256;
            testEntriesMaxSize(s, 64000, 79800, i);
            testEntriesMaxSize(s, 129000, 160000, i);
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    double score = 0;
    int scoreCount = 0;

    private void testEntriesMaxSize(int segments, int minSize, int maxSize, int seed) throws IOException {
        int counter = minSize + new Random(seed + segments * 1000).nextInt(9999);
        ChronicleMap<CharSequence, LongValue> map = getSharedMap(minSize, segments, 32);
        LongValue longValue = DirectLongValueFactory.INSTANCE.create();
        try {
            for (int j = 0; j < maxSize * 12 / 10 + 300; j++) {
                String key = "key:" + counter++;
                // give a biased hashcode distribution.
                if ((Integer.bitCount(key.hashCode()) & 7) != 0) {
                    j--;
                    continue;
                }
                map.acquireUsing(key, longValue);
                longValue.setValue(1);
            }
            int[] a = ((VanillaChronicleMap) map).segmentSizes();
            System.out.println("segs: " + segments + " min: " + minSize + " was " + map.size() + " "
                    + Arrays.toString(a)
                    + " sum: " + sum(a));
            fail("Expected the map to be full.");
        } catch (IllegalStateException e) {
            // calculate the hyperbolic average.
            score += (double) minSize / map.size();
            scoreCount++;
            boolean condition = minSize <= map.size() && map.size() <= maxSize;
            if (!condition) {
                int[] a = ((VanillaChronicleMap) map).segmentSizes();
                System.out.println("segs: " + segments + " min: " + minSize + " was " + map.size() + " "
                        + Arrays.toString(a)
                        + " sum: " + sum(a));
                assertTrue("min: " + minSize + ", size: " + map.size(), condition);
            }
        } finally {
            map.close();
        }
    }

    private long sum(int[] ints) {
        long sum = 0;
        for (int i : ints) {
            sum += i;
        }
        return sum;
    }

    static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/small-chm-test" + System.nanoTime());
        file.deleteOnExit();
        return file;
    }

    private static ChronicleMap<CharSequence, LongValue> getSharedMap(
            long entries, int segments, int entrySize) throws IOException {
        return getSharedMap(entries, segments, entrySize, OF_4_BYTES);
    }

    private static ChronicleMap<CharSequence, LongValue> getSharedMap(
            long entries, int segments, int entrySize, Alignment alignment)
            throws IOException {
        ChronicleMapBuilder<CharSequence, LongValue> mapBuilder = ChronicleMapBuilder.of(CharSequence.class, LongValue.class)
                .entries(entries)
                .minSegments(segments)
                .entrySize(entrySize)
                .entryAndValueAlignment(alignment)
                .valueMarshallerAndFactory(ByteableLongValueMarshaller.INSTANCE,
                        DirectLongValueFactory.INSTANCE)
                .file(getPersistenceFile());
        return mapBuilder.create();
    }


}
