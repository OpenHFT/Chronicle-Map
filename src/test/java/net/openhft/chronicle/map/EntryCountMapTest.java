/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.values.LongValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static net.openhft.chronicle.map.Alignment.OF_4_BYTES;
import static net.openhft.lang.model.DataValueClasses.newDirectReference;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EntryCountMapTest {
    double score = 0;
    int scoreCount = 0;

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
        ChronicleMapBuilder<CharSequence, LongValue> mapBuilder = ChronicleMapBuilder.of(CharSequence
                .class, LongValue.class)
                .entries(entries)
                .actualSegments(segments)
                .entrySize(entrySize)
                .entryAndValueAlignment(alignment);
        return mapBuilder.createPersistedTo(getPersistenceFile());
    }

    @Test
    public void testVerySmall() throws Exception {
        for (int t = 0; t < 5; t++) {
            int s = 1;
            // regression test.
            testEntriesMaxSize(s, 1, 1);
            testEntriesMaxSize(s, 2, 2);
            testEntriesMaxSize(s, 4, 4);
            testEntriesMaxSize(s, 8, 8);
            testEntriesMaxSize(s, 16, 16);
            testEntriesMaxSize(s, 32, 32);
            testEntriesMaxSize(s, 64, 64);
            testEntriesMaxSize(s, 128, 128);
            testEntriesMaxSize(s, 256, 256);
            testEntriesMaxSize(s, 512, 512);

            s = 2;
            // regression test.
            testEntriesMaxSize(s, 8, 16);
            testEntriesMaxSize(s, 16, 32);
            testEntriesMaxSize(s, 32, 56);
            testEntriesMaxSize(s, 64, 95);
            testEntriesMaxSize(s, 128, 168);
            testEntriesMaxSize(s, 256, 322);
            testEntriesMaxSize(s, 512, 640);

            s = 4;
            // regression test.
            testEntriesMaxSize(s, 32, 70);
            testEntriesMaxSize(s, 64, 120);
            testEntriesMaxSize(s, 128, 200);
            testEntriesMaxSize(s, 256, 380);
            testEntriesMaxSize(s, 512, 650);

            for (int s2 : new int[]{1, 2, 4}) {
                testEntriesMaxSize(s2, 1000, 1300);
                testEntriesMaxSize(s2, 2000, 2500);
                testEntriesMaxSize(s2, 4000, 5000);
                testEntriesMaxSize(s2, 5000, 6200);
                testEntriesMaxSize(s2, 8000, 9900);
                testEntriesMaxSize(s2, 12000, 15000);
                testEntriesMaxSize(s2, 16000, 20000);
            }
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    @Test
    public void testSmall() throws Exception {
        for (int i = 0; i < 5; i++) {
            {
                int s = 8;
                testEntriesMaxSize(s, 250, 400);
                testEntriesMaxSize(s, 500, 770);
                testEntriesMaxSize(s, 1000, 1400);
                testEntriesMaxSize(s, 2000, 2800);
            }
            // regression test.
            for (int s : new int[]{8, 16}) {
                testEntriesMaxSize(s, 3000, 4200);
                testEntriesMaxSize(s, 5000, 6900);
                testEntriesMaxSize(s, 10000, 13000);
            }

            for (int s : new int[]{8, 16, 32}) {
                testEntriesMaxSize(s, 8000, 11600);
                testEntriesMaxSize(s, 12000, 16500);
                testEntriesMaxSize(s, 16000, 22000);
                testEntriesMaxSize(s, 24000, 31000);
                testEntriesMaxSize(s, 32000, 41000);
            }
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    @Ignore("Long running test")
    @Test
    public void testMedium() throws Exception {
        for (int i = 0; i < 10; i++) {
            // regression test.
            for (int s : new int[]{16, 32, 64}) {
                int s3 = s * s * s;
                testEntriesMaxSize(s, s3, s3 * 14 / 10);
                testEntriesMaxSize(s, s3 * 3 / 2, s3 * 2);
                testEntriesMaxSize(s, s3 * 2, s3 * 27 / 10);
            }
            for (int s : new int[]{128, 256, 512}) {
                int s2 = s * s;
                testEntriesMaxSize(s, s2, s2 * 15 / 10);
            }
            for (int s : new int[]{1024, 2048, 4096}) {
                int s2 = s * 1024;
                testEntriesMaxSize(s, s2, s2 * 14 / 10);
            }
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    @Test
    @Ignore("ignore because its a scalability test")
    public void testLarge() throws Exception {
        int s = 8 * 1024;
        for (int i = 0; i < 10; i++) {
            System.out.println("i:" + i);
            int entries = s * s;
            testEntriesMaxSize(s, entries * 4 / 5, entries * 4 / 3);
//            testEntriesMaxSize(s, entries + 100, entries * 3 / 2, i);
            entries *= 2;
            testEntriesMaxSize(s, entries * 4 / 5, entries * 4 / 3);
//            testEntriesMaxSize(s, entries + 100, entries * 3 / 2, i);
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    private void testEntriesMaxSize(int segments, int minSize, int maxSize) throws IOException {
        assert minSize <= maxSize;
        Random random = new Random();
        int counter = minSize + random.nextInt(9999 + maxSize);
        int stride = 1 + random.nextInt(100);
        ChronicleMap<CharSequence, LongValue> map = getSharedMap(minSize, segments, 32);
        LongValue longValue = newDirectReference(LongValue.class);
        try {
            for (int j = 0; j < maxSize * 14 / 10 + 300; j++) {
                String key = "key:" + counter;
                counter += stride;
                // give a biased hashcode distribution.
                if ((Integer.bitCount(key.hashCode()) & 7) != 0) {
                    j--;
                    continue;
                }
                map.acquireUsing(key, longValue);
                longValue.setValue(1);
            }
            long[] a = ((VanillaChronicleMap) map).segmentSizes();
            System.out.println("segs: " + segments + " min: " + minSize + " was " + map.size() + " "
                    + Arrays.toString(a)
                    + " sum: " + sum(a));
            fail("Expected the map to be full.");
        } catch (IllegalStateException e) {
            // calculate the hyperbolic average.
            score += (double) minSize / map.size();
            scoreCount++;
            boolean condition = minSize <= map.size() && map.size() <= maxSize;
            long[] a = ((VanillaChronicleMap) map).segmentSizes();
            System.out.println("segs: " + segments + " min: " + minSize + " was " + map.size() + " "
                    + Arrays.toString(a)
                    + " sum: " + sum(a));
            if (!condition) {
                assertTrue("stride: " + stride + ", seg: " + segments + ", min: " + minSize + ", size: " + map.size(), condition);
            }
        } finally {
            map.close();
        }
    }

    private long sum(long[] longs) {
        long sum = 0;
        for (long i : longs) {
            sum += i;
        }
        return sum;
    }

}
