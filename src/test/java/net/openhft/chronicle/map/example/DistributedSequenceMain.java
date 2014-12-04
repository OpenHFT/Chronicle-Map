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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.io.File;
import java.io.IOException;

public class DistributedSequenceMain {

    public static void main(String... ignored) throws IOException {
        File file = File.createTempFile("sequence-numbers", ".dat");
        try (ChronicleMap<String, LongValue> map =
                     ChronicleMapBuilder.of(String.class, LongValue.class)
                             .entries(128)
                             .actualSegments(1)
                             .createPersistedTo(file)) {
            for (int t = 0; t < 5; t++) {
                LongValue value = DataValueClasses.newDirectReference(LongValue.class);
                map.acquireUsing("sequence-" + t, value);

                long start = System.nanoTime();
                int runs = 10000000;
                for (int i = 0; i < runs; i++) {
                    long nextId = value.addAtomicValue(1);
                }
                double rate = (double) (System.nanoTime() - start) / runs;
                System.out.printf("Uncontended increment took an average of %.1f ns.%n", rate);
            }
        }
    }
}
