/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.algo.MemoryUnit.BYTES;
import static net.openhft.chronicle.algo.MemoryUnit.MEGABYTES;

public class VinceRun {
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
