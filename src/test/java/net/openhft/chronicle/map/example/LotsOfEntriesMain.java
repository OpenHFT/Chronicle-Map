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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

interface MyFloats {
    @Array(length = 6)
    public void setValueAt(int index, float f);

    public float getValueAt(int index);
}

/**
 * Created by peter.lawrey on 19/12/14.
 */
public class LotsOfEntriesMain {
    public static void main(String[] args)
            throws IOException, ExecutionException, InterruptedException {
        workEntries(true);
        workEntries(false);
    }

    private static void workEntries(final boolean add)
            throws IOException, ExecutionException, InterruptedException {
        final long entries = 100_000_000;
        File file = new File("/tmp/lotsOfEntries.dat");
        final ChronicleMap<CharSequence, MyFloats> map = ChronicleMapBuilder
                .of(CharSequence.class, MyFloats.class)
                .entries(entries)
                // + 2 is average oversize because we append 4-letter "-key" in a loop
                .averageKeySize((Math.log(1.024) - Math.log(0.024)) * 24 + 2)
                .createPersistedTo(file);
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService es = Executors.newFixedThreadPool(threads);
        long block = (entries + threads - 1) / threads;

        final long start = System.nanoTime();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final long startI = t * block;
            final long endI = Math.min((t + 1) * block, entries);
            futures.add(es.submit(new Runnable() {
                @Override
                public void run() {
                    Random rand = new Random(startI);
                    StringBuilder sb = new StringBuilder();
                    MyFloats mf = Values.newHeapInstance(MyFloats.class);
                    if (add)
                        for (int i = 0; i < 6; i++)
                            mf.setValueAt(i, i);
                    for (long i = startI; i < endI; i++) {
                        sb.setLength(0);
                        int length = (int) (24 / (rand.nextFloat() + 24.0 / 1000));
                        sb.append(i);
                        while (sb.length() < length)
                            sb.append("-key");
                        try {
                            if (add)
                                map.put(sb, mf);
                            else
                                map.getUsing(sb, mf);
                        } catch (Exception e) {
                            System.out.println("map.size: " + map.size());
                            throw e;
                        }
                    }
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        long time = System.nanoTime() - start;
        es.shutdown();
        System.out.printf("Map.size: %,d with a throughput of %.1f million/sec to %s.%n",
                map.size(), entries * 1e3 / time, add ? "add" : "get");
        map.close();
    }
}
