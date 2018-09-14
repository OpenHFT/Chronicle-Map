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

import net.openhft.chronicle.core.values.LongValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.lang.Math.log10;
import static java.lang.Math.round;
import static net.openhft.chronicle.values.Values.newNativeReference;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EntryCountMapTest {
    static final String TMP = System.getProperty("java.io.tmpdir");
    static final int ecmTests = Integer.getInteger("ecm.tests", 5);
    double score = 0;
    int scoreCount = 0;

    static File getPersistenceFile() throws IOException {
        File file = File.createTempFile("ecm-chm-test", ".deleteme");
        file.deleteOnExit();
        return file;
    }

    private static ChronicleMap<CharSequence, LongValue> getSharedMap(
            long entries, int segments, int keySize) throws IOException {
        ChronicleMapBuilder<CharSequence, LongValue> mapBuilder =
                ChronicleMapBuilder.of(CharSequence.class, LongValue.class)
                        .allowSegmentTiering(false)
                        .entries(entries)
                        .actualSegments(segments)
                        .averageKeySize(keySize);
        return mapBuilder.createPersistedTo(getPersistenceFile());
    }

    private static int moreThanMaxSize(int maxSize) {
        return maxSize * 14 / 10 + 300;
    }

    @Ignore("HCOLL-279 fix net.openhft.chronicle.map.EntryCountMapTest#testVerySmall")
    @Test
    public void testVerySmall() throws IOException {
        System.out.print("testVerySmall seeds");
        for (int t = 0; t < ecmTests; t++) {
            System.out.print(".");
            int s = 1;
            // regression test.
            testEntriesMaxSize(s, 1, 1, t);
            testEntriesMaxSize(s, 2, 2, t);
            testEntriesMaxSize(s, 4, 4, t);
            testEntriesMaxSize(s, 8, 8, t);
            testEntriesMaxSize(s, 16, 16, t);
            testEntriesMaxSize(s, 32, 32, t);
            testEntriesMaxSize(s, 64, 64, t);
            testEntriesMaxSize(s, 128, 128, t);
            testEntriesMaxSize(s, 256, 256, t);
            testEntriesMaxSize(s, 512, 512, t);

            s = 2;
            // regression test.
            testEntriesMaxSize(s, 8, 16, t);
            testEntriesMaxSize(s, 16, 38, t);
            testEntriesMaxSize(s, 32, 64, t);
            testEntriesMaxSize(s, 64, 95, t);
            testEntriesMaxSize(s, 128, 168, t);
            testEntriesMaxSize(s, 256, 322, t);
            testEntriesMaxSize(s, 512, 640, t);

            s = 4;
            // regression test.
            testEntriesMaxSize(s, 32, 72, t);
            testEntriesMaxSize(s, 64, 120, t);
            testEntriesMaxSize(s, 128, 200, t);
            testEntriesMaxSize(s, 256, 380, t);
            testEntriesMaxSize(s, 512, 650, t);

            for (int s2 : new int[]{1, 2, 4}) {
                testEntriesMaxSize(s2, 1000, 1300, t);
                testEntriesMaxSize(s2, 2000, 2500, t);
                testEntriesMaxSize(s2, 4000, 5000, t);
                testEntriesMaxSize(s2, 5000, 6200, t);
                testEntriesMaxSize(s2, 8000, 9900, t);
                testEntriesMaxSize(s2, 12000, 15000, t);
                testEntriesMaxSize(s2, 16000, 20000, t);
            }
        }
        // hyperbolic average gives more weight to small numbers.
        System.out.printf(" Score: %.2f%n", scoreCount / score);
    }

    @Test
    public void testSmall() throws IOException, ExecutionException, InterruptedException {
        System.out.print("testSmall seeds");
        int procs = Runtime.getRuntime().availableProcessors();
        ExecutorService es = Executors.newFixedThreadPool(procs);
        for (int t = 0; t < ecmTests; t++) {
            List<Future<?>> futures = new ArrayList<>();
            {
                int s = 8;
                futures.add(testEntriesMaxSize(es, s, 250, 450, t));
                futures.add(testEntriesMaxSize(es, s, 500, 840, t));
                futures.add(testEntriesMaxSize(es, s, 1000, 1550, t));
                futures.add(testEntriesMaxSize(es, s, 2000, 3200, t));
            }
            // regression test.
            for (int s : new int[]{8, 16}) {
                futures.add(testEntriesMaxSize(es, s, 3000, 5000, t));
                futures.add(testEntriesMaxSize(es, s, 5000, 7500, t));
                futures.add(testEntriesMaxSize(es, s, 10000, 15000, t));
            }

            for (int s : new int[]{32, 64, 128})
                futures.add(testEntriesMaxSize(es, s, 250 * s, 250 * s * 5 / 3, t));

            int s = 32;
            for (int e : new int[]{16000, 25000, 50000})
                futures.add(testEntriesMaxSize(es, s, e, e * 5 / 3, t));
            s = 64;
            for (int e : new int[]{25000, 50000, 100000})
                futures.add(testEntriesMaxSize(es, s, e, e * 5 / 3, t));
            s = 128;
            for (int e : new int[]{50000, 100000, 200000})
                futures.add(testEntriesMaxSize(es, s, e, e * 5 / 3, t));
            for (Future<?> future : futures) {
                System.out.print(".");
                future.get();
            }
        }
        es.shutdown();
        // hyperbolic average gives more weight to small numbers.
        System.out.printf(" Score: %.2f%n", scoreCount / score);
    }

    @Ignore("Long running, large tests test")
    @Test
    public void testMedium() throws IOException, ExecutionException, InterruptedException {
        System.out.print("testMedium seeds");
        int procs = Runtime.getRuntime().availableProcessors();
        ExecutorService es = Executors.newFixedThreadPool(procs);
        for (int t = 0; t < ecmTests; t++) {
            // regression test.
            List<Future<?>> futures = new ArrayList<>();
            for (int s : new int[]{512, 256, 128, 64}) {
                int s3 = s * s * 128;
                futures.add(testEntriesMaxSize(es, s, s3 * 2, s3 * 24 / 10, t));
                if (s < 512) {
                    futures.add(testEntriesMaxSize(es, s, s3 * 3 / 2, s3 * 9 / 5, t));
                    futures.add(testEntriesMaxSize(es, s, s3, s3 * 12 / 10, t));
                }
            }
            for (Future<?> future : futures) {
                System.out.print(".");
                future.get();
            }
        }
        es.shutdown();
        // hyperbolic average gives more weight to small numbers.
        System.out.printf("Score: %.2f%n", scoreCount / score);
    }

    private Future<Void> testEntriesMaxSize(ExecutorService es, final int segments,
                                            final int minSize, final int maxSize, final int seed)
            throws IOException {
        assert minSize <= maxSize;
        Random random = new Random(seed);
        int counter = minSize + random.nextInt(9999 + maxSize);
        final int stride = 1 + random.nextInt(100);
        final int maxKeySize = "key:".length() +
                (int) round(log10(moreThanMaxSize(maxSize) * stride + counter)) + 1;

        return es.submit(new Callable<Void>() {
            @Override
            public Void call() {
                File f = null;
                try (final ChronicleMap<CharSequence, LongValue> map =
                             getSharedMap(minSize, segments, maxKeySize)) {
                    f = map.file();
                    testEntriesMaxSize0(segments, minSize, maxSize, seed, stride, map);
                } finally {
                    if (f != null && f.exists())
                        f.delete();
                    return null;
                }
            }
        });

    }

    void testEntriesMaxSize(int segments, int minSize, int maxSize, int seed) throws IOException {
        assert minSize <= maxSize;
        Random random = new Random(seed);
        int counter = minSize + random.nextInt(9999 + maxSize);
        int stride = 1 + random.nextInt(100);
        int maxKeySize = "key:".length() +
                (int) round(log10(moreThanMaxSize(maxSize) * stride + counter)) + 1;
        File file;
        try (ChronicleMap<CharSequence, LongValue> map = getSharedMap(minSize, segments, maxKeySize)) {
            file = map.file();
            testEntriesMaxSize0(segments, minSize, maxSize, counter, stride, map);
        }
        file.delete();
    }

    void testEntriesMaxSize0(int segments, int minSize, int maxSize, int counter, int stride,
                             ChronicleMap<CharSequence, LongValue> map) {
        LongValue longValue = newNativeReference(LongValue.class);
        try {
            for (int j = 0; j < moreThanMaxSize(maxSize); j++) {
                String key = "key:" + counter;
                if (minSize > 10 && (counter & 15) == 7)
                    key += "-oversized-key";
                counter += stride;
                // give a biased hashcode distribution.
                if ((Integer.bitCount(key.hashCode()) & 7) != 0) {
                    j--;
                    continue;
                }
                map.acquireUsing(key, longValue);
                longValue.setValue(1);
            }
            dumpMapStats(segments, minSize, map);
            fail("Expected the map to be full.");
        } catch (IllegalStateException e) {
            // calculate the hyperbolic average.
            score += (double) minSize / map.size();
            scoreCount++;
            boolean condition = minSize <= map.size() && map.size() <= minSize * 2 + 8;
            if (!condition) {
                dumpMapStats(segments, minSize, map);
                assertTrue("stride: " + stride + ", seg: " + segments + ", min: " + minSize +
                        ", size: " + map.size(), condition);
            } else if (map.size() > maxSize)
                System.err.println(" warning, larger than expected, stride: " + stride +
                        ", seg: " + segments + ", min: " + minSize +
                        ", size: " + map.size());
        }
    }

    private void dumpMapStats(int segments, int minSize,
                              ChronicleMap<CharSequence, LongValue> map) {
        long[] a = new long[map.segments()];
        for (int i = 0; i < map.segments(); i++) {
            try (MapSegmentContext<CharSequence, LongValue, ?> c = map.segmentContext(i)) {
                a[i] = c.size();
            }
        }
        System.out.println("segs: " + segments + " min: " + minSize + " was " + map.size() + " "
                + Arrays.toString(a)
                + " sum: " + sum(a));
    }

    private long sum(long[] longs) {
        long sum = 0;
        for (long i : longs) {
            sum += i;
        }
        return sum;
    }

}
