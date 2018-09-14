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

package examples.portfolio;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapSegmentContext;
import net.openhft.chronicle.values.Values;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PortfolioValueTest {
    private static final boolean useIterator = true;
    private static final long nAssets = 10_000_000;
    private static final int nThreads = Runtime.getRuntime().availableProcessors();
    private static final int nRepetitions = 11; // 10 for computing average, throwing away the first one (warmup)

    private static void computeValue(final ChronicleMap<LongValue, PortfolioAssetInterface> cache) throws ExecutionException, InterruptedException {
        computeValueUsingIterator(cache);
    }

    private static void computeValueUsingIterator(final ChronicleMap<LongValue, PortfolioAssetInterface> cache) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        @SuppressWarnings("unchecked")
        Future<Double>[] futures = new Future[nThreads];
        long batchSize = (useIterator ? cache.segments() : nAssets) / nThreads;

        // Map
        for (int t = 0; t < nThreads; t++) {
            final int batch = t;

            futures[t] = executor.submit(() -> {
                if (useIterator) {
                    int start = (int) (batch * batchSize);
                    int end = (int) Math.min(cache.segments(), (batch + 1) * batchSize);
                    return computeTotalUsingIterator(cache, start, end);
                } else {
                    long start = batch * batchSize;
                    long end = Math.min(nAssets, (batch + 1) * batchSize);
                    return computeTotalUsingKeys(cache, start, end);
                }
            });
        }

        // Reduce
        double total = 0;
        for (Future<Double> future : futures) {
            total += future.get();
        }

        long elapsedTime = (System.currentTimeMillis() - startTime);
        System.out.println("Total Portfolio Value: " + total + " for " + cache.longSize() + ", computed in " + elapsedTime + " ms, using " + (useIterator ? "Iterator" : "Keys"));
    }

    protected static double computeTotalUsingIterator(final ChronicleMap<LongValue, PortfolioAssetInterface> cache, int start, int end) {
        if (end > start) {
            final PortfolioAssetInterface asset = Values.newHeapInstance(PortfolioAssetInterface.class);
            PortfolioValueAccumulator accumulator = new PortfolioValueAccumulator(new MutableDouble(), asset);
            for (int s = start; s < end; s++) {
                try (MapSegmentContext<LongValue, PortfolioAssetInterface, ?> context = cache.segmentContext(s)) {
                    context.forEachSegmentEntry(accumulator);
                }
            }
            return accumulator.total.doubleValue();
        }
        return 0;
    }

    protected static double computeTotalUsingKeys(final ChronicleMap<LongValue, PortfolioAssetInterface> cache, long start, long end) {
        final LongValue key = Values.newHeapInstance(LongValue.class);
        PortfolioAssetInterface asset = Values.newHeapInstance(PortfolioAssetInterface.class);

        double total = 0;
        for (long k = start; k < end; k++) {
            key.setValue(k);
            asset = cache.getUsing(key, asset);
            total += asset.getShares() * asset.getPrice();
        }
        return total;
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        ChronicleMapBuilder<LongValue, PortfolioAssetInterface> mapBuilder = ChronicleMapBuilder.of(LongValue.class, PortfolioAssetInterface.class).entries(nAssets);

        try (ChronicleMap<LongValue, PortfolioAssetInterface> cache = mapBuilder.create()) {
            createData(cache);

            // Compute multiple times to get an reasonable average compute time
            for (int i = 0; i < nRepetitions; i++) {
                computeValue(cache);
            }
        }
    }

    private void createData(final ChronicleMap<LongValue, PortfolioAssetInterface> cache) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        Future<?>[] futures = new Future[nThreads];
        long batchSize = nAssets / nThreads;

        for (int t = 0; t < nThreads; t++) {
            final long batch = t;
            futures[t] = executor.submit(() -> {
                final LongValue key = Values.newHeapInstance(LongValue.class);
                final PortfolioAssetInterface value = Values.newHeapInstance(PortfolioAssetInterface.class);

                long start = batch * batchSize;
                long end = Math.min(nAssets, (batch + 1) * batchSize);
                long n = (end - start);

                if (end > start) {
                    System.out.println("Inserting batch " + (batch + 1) + "/" + nThreads + " of " + n + " records");

                    for (long k = start; k < end; k++) {
                        key.setValue(k);
                        value.setAssetId(k);
                        value.setShares(1);
                        value.setPrice(2.0);
                        cache.put(key, value);
                    }
                }
            });
        }

        for (Future<?> future : futures) {
            future.get();
        }

        long elapsedTime = (System.currentTimeMillis() - startTime);
        System.out.println("Data inserted in " + elapsedTime + " ms");
    }
}
