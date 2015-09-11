/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

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

    private static void test(Function<ChronicleMapBuilder<Integer, Integer>,
            ChronicleMap<Integer, Integer>> createMap) {
        long ms = System.currentTimeMillis();
        try (ChronicleMap<Integer, Integer> map = createMap.apply(ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(ENTRIES)
                .entriesPerSegment((1 << 17) / 3)
                .putReturnsNull(true)
                .removeReturnsNull(true))) {

            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < ENTRIES; i++) {
                map.put(r.nextInt(), r.nextInt());
            }
        }
        System.out.println(System.currentTimeMillis() - ms);
    }

    @Test(timeout = 25000)
    public void inMemoryTest() {
        test(new Function<ChronicleMapBuilder<Integer, Integer>, ChronicleMap<Integer, Integer>>() {
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
            test(new Function<ChronicleMapBuilder<Integer, Integer>,
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
}
