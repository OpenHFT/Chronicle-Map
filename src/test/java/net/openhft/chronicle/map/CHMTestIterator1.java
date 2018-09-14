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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Anshul Shelley
 */

public class CHMTestIterator1 {
    public static void main(String[] args) {
        AtomicLong alValue = new AtomicLong();
        AtomicLong alKey = new AtomicLong();
        int runs = 3000000;
        ChronicleMapBuilder<String, Long> builder = ChronicleMapBuilder.of(String.class, Long.class)

                .entries(runs);

        try (ChronicleMap<String, Long> chm = builder.create()) {
        /*chm.put("k1", alValue.incrementAndGet());
        chm.put("k2", alValue.incrementAndGet());
        chm.put("k3", alValue.incrementAndGet());
        chm.put("k4", alValue.incrementAndGet());
        chm.put("k5", alValue.incrementAndGet());*/
            //chm.keySet();

            for (int i = 0; i < runs; i++) {
                chm.put("k" + alKey.incrementAndGet(), alValue.incrementAndGet());
            }

            long start = System.nanoTime();
            for (Map.Entry<String, Long> entry : chm.entrySet()) {
                entry.getKey();
                entry.getValue();
            }
            long time = System.nanoTime() - start;
            System.out.println("Average iteration time was " + time / runs / 1e3 + "us, for " + runs / 1e6 + "m entries");
        }
    }

}