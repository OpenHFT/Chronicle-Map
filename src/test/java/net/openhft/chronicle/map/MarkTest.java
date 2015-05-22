/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.lang.MemoryUnit;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertTrue;

public class MarkTest {

    static int ENTRIES = 25_000_000;

    @Test(timeout = 25000)
    public void inMemoryTest() {
        test(new SerializableFunction<ChronicleMapBuilder<Integer, Integer>, ChronicleMap<Integer, Integer>>() {
            @Override
            public ChronicleMap<Integer, Integer> apply(
                    ChronicleMapBuilder<Integer, Integer> builder) {
                return builder.create();
            }
        });
    }

    @Test(timeout = 25000)
    public void persistedTest() {
        int rnd = new Random().nextInt();
        final File db = Paths.get(System.getProperty("java.io.tmpdir"), "mark" + rnd).toFile();
        if (db.exists())
            db.delete();
        try {
            test(new SerializableFunction<ChronicleMapBuilder<Integer, Integer>,
                                ChronicleMap<Integer, Integer>>() {
                @Override
                public ChronicleMap<Integer, Integer> apply(
                        ChronicleMapBuilder<Integer, Integer> builder) {
                    try {
                        return builder.createPersistedTo(db);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            System.out.println(MemoryUnit.BYTES.toMegabytes(db.length()) + " MB");
            assertTrue("ChronicleMap of 25 million int-int entries should be lesser than 400MB",
                    db.length() < MemoryUnit.MEGABYTES.toBytes(400));
        } finally {
            db.delete();
        }
    }

    private static void test(SerializableFunction<ChronicleMapBuilder<Integer, Integer>,
                ChronicleMap<Integer, Integer>> createMap) {
        long ms = System.currentTimeMillis();
        try (ChronicleMap<Integer, Integer> map = createMap.apply(ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(ENTRIES)
                .entriesPerSegment((1 << 15) / 3)
                .putReturnsNull(true)
                .removeReturnsNull(true))) {
            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < ENTRIES; i++) {
                map.put(r.nextInt(), r.nextInt());
            }
        }
        System.out.println(System.currentTimeMillis() - ms);
    }
}
