/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.MemoryUnit;
import net.openhft.chronicle.core.OS;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;

class MarkTest {

    static int ENTRIES = 25_000_000;

    private static void test(
            Function<ChronicleMapBuilder<Integer, Integer>, ChronicleMap<Integer, Integer>>
                    createMap) {
        long ms = System.currentTimeMillis();
        try (ChronicleMap<Integer, Integer> map = createMap.apply(ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(ENTRIES)
                .entriesPerSegment((1 << 15) / 3)
                .checksumEntries(false)
                .putReturnsNull(true)
                .removeReturnsNull(true))) {

            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < ENTRIES; i++) {
                map.put(r.nextInt(), r.nextInt());
            }
        }
        System.out.println(System.currentTimeMillis() - ms);
    }

    @Disabled("often out of time, that is a parf issue, not a bug")
    @Test
    @Timeout(value = 25000, unit = TimeUnit.MILLISECONDS)
    void inMemoryTest() {
        test(ChronicleMapBuilder::create);
    }

    @Disabled("ignored because it take too long and times out")
    @Test
    @Timeout(value = 25000, unit = TimeUnit.MILLISECONDS)
    void persistedTest() {
        int rnd = new Random().nextInt();
        final File db = Paths.get(OS.getTarget(), "mark" + rnd).toFile();
        if (db.exists())
            db.delete();
        try {
            test(builder -> {
                try {
                    return builder.createPersistedTo(db);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            System.out.println(MemoryUnit.BYTES.toMegabytes(db.length()) + " MB");
            assertTrue(db.length() < MemoryUnit.MEGABYTES.toBytes(400), "ChronicleMap of 25 million int-int entries should be lesser than 400MB");
        } finally {
            db.delete();
        }
    }

    @Test
    void testNegativeEntriesPerSegment() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChronicleMapBuilder.of(Integer.class, Integer.class).entriesPerSegment(-1);
        });
    }
}
