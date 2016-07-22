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

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class DistributedSequenceMain {

    public static void main(String... ignored) throws IOException {
        File file = File.createTempFile("sequence-numbers", ".dat");
        try (ChronicleMap<String, LongValue> map =
                     ChronicleMapBuilder.of(String.class, LongValue.class)
                             .entries(128)
                             .actualSegments(1)
                             .createPersistedTo(file)) {
            // throughput test.
            for (int t = 0; t < 5; t++) {
                LongValue value = Values.newNativeReference(LongValue.class);
                map.acquireUsing("sequence-" + t, value);

                long start = System.nanoTime();
                int runs = 1000000;
                for (int i = 0; i < runs; i++) {
                    long nextId = value.addAtomicValue(1);
                }
                double rate = (double) (System.nanoTime() - start) / runs;
                System.out.printf("Uncontended increment took an average of %.1f ns.%n", rate);
            }

            // latency test.
            LongValue value = Values.newNativeReference(LongValue.class);
            map.acquireUsing("sequence-X", value);

            int runs = 1000000000, count = 0, wcount = 0;
            long[] worst = new long[200000];
            long start = System.nanoTime();
            for (; count < runs; count++) {
                long nextId = value.addAtomicValue(1);
                long next = System.nanoTime();
                long time = next - start;
                if (time > 100) {
                    worst[wcount++] = time;
                    if (wcount >= worst.length)
                        break;
                    next = System.nanoTime();
                }
                start = next;
            }
            Arrays.sort(worst);
            System.out.printf("Worst 1 in 10K, 1 in 100K, 1 in 1000K, worst is %,d / %,d / %,d / %,d %n",
                    worst[wcount - count / 10000],
                    worst[wcount - count / 100000],
                    worst[wcount - count / 1000000],
                    worst[wcount - 1]
            );
        }
    }
}

