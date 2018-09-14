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
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.algo.MemoryUnit.BYTES;
import static net.openhft.chronicle.algo.MemoryUnit.MEGABYTES;

public class VinceTest {
    public static void main(String[] args) throws IOException {
        long count = 100_000_000L;
        File f = File.createTempFile("vince", ".map");
        f.deleteOnExit();
        try (ChronicleMap<CharSequence, LongValue> catalog = ChronicleMap
                .of(CharSequence.class, LongValue.class)
                .entries(count)
                .averageKey("100000000")
                .putReturnsNull(true)
                .createPersistedTo(f)) {

            long prev = System.currentTimeMillis();

            StringBuilder key = new StringBuilder();
            LongValue value = Values.newHeapInstance(LongValue.class);

            for (long i = 1; i <= count; i++) {
                key.setLength(0);
                key.append(i);
                value.setValue(i);
                catalog.put(key, value);
                if ((i % 1_000_000) == 0) {
                    long now = System.currentTimeMillis();
                    System.out.printf("Average ns to insert per mi #%d: %d\n",
                            (i / 1_000_000), now - prev);
                    prev = now;
                }
            }
            System.out.println("file size " + MEGABYTES.convert(f.length(), BYTES) + " MB");
        }
    }
}
