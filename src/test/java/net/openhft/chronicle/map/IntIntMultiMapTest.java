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

package net.openhft.chronicle.map;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.DirectStore;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: peter Date: 09/12/13
 */
@RunWith(value = Parameterized.class)
public class IntIntMultiMapTest {

    MultiMap map;
    Multimap<Long, Long> referenceMap = HashMultimap.create();
    private Class<? extends MultiMap> c;

    public IntIntMultiMapTest(Class<? extends MultiMap> c)
            throws Exception {
        this.c = c;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {VanillaShortShortMultiMap.class},
                {VanillaIntIntMultiMap.class},
        });
    }

    private void initMap(long capacity) {
        try {
            map = c.getConstructor(long.class).newInstance(capacity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void multiMapEquals() {
        class Action implements MultiMap.EntryConsumer {
            int mapSize = 0;

            @Override
            public void accept(long key, long value) {
                mapSize++;
                assertTrue(referenceMap.containsEntry(key, value));
            }
        }
        Action action = new Action();
        map.forEach(action);
        assertEquals(referenceMap.size(), action.mapSize);
    }

    private void valuesEqualsByKey(long k) {
        List<Long> values = new ArrayList<Long>();
        map.startSearch(k);
        long v;
        while ((v = map.nextPos()) >= 0L)
            values.add(v);
        Set<Long> valueSet = new HashSet<Long>(values);
        assertEquals(values.size(), valueSet.size());
        assertEquals(new HashSet<Long>(referenceMap.get(k)), valueSet);
    }

    private void put(long k, long v) {
        map.put(k, v);
        referenceMap.put(k, v);
    }

    private void remove(long k, long v) {
        map.remove(k, v);
        referenceMap.remove(k, v);
    }

    @Test
    public void testPutRemoveSearch() {
        initMap(16);
        multiMapEquals();
        put(1, 11);
        valuesEqualsByKey(1);

        multiMapEquals();
        put(3, 33);
        multiMapEquals();
        put(1, 12);
        put(1, 13);
        put(1, 14);
        put(3, 32);
        put(1, 15);
        multiMapEquals();

        remove(1, 11);
        multiMapEquals();

        valuesEqualsByKey(3);
        valuesEqualsByKey(1);

        remove(1, 12);
        multiMapEquals();

        remove(1, 15);
        multiMapEquals();

        remove(1, 13);
        multiMapEquals();

        remove(1, 14);
        multiMapEquals();
    }

    @Test
    public void testRemoveSpecific() {
        // Testing a specific case when the remove method on the map
        // does (did) not work as expected. The size goes correctly to
        // 0 but the value is still present in the map.
        initMap(10);

        map.put(15, 1);
        map.remove(15, 1);
        map.startSearch(15);
        assertTrue(map.nextPos() < 0);
    }

    @Test
    @Ignore("Very long running test")
    public void testMaxCapacity() throws NoSuchFieldException, IllegalAccessException {
        Field maxCapacityField = c.getDeclaredField("MAX_CAPACITY");
        long maxCapacity = maxCapacityField.getLong(null);
        Field entrySizeField = c.getDeclaredField("ENTRY_SIZE");
        entrySizeField.setAccessible(true);
        long entrySize = entrySizeField.getLong(null);
        if (maxMemory() * 1024L < maxCapacity * entrySize * 3L / 2L) {
            System.out.println("Skipped " + c + " maxCapacity test because there is not enough " +
                    "memory in system");
            return;
        }
        initMap(maxCapacity);
        Random r = new Random();
        int bound = maxCapacity > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxCapacity;
        for (long i = 0L; i < maxCapacity; i++) {
            map.put(r.nextInt(bound), i);
        }
        final SingleThreadedDirectBitSet bs =
                new SingleThreadedDirectBitSet(new DirectStore(maxCapacity / 8).bytes());
        map.forEach(new MultiMap.EntryConsumer() {
            @Override
            public void accept(long key, long value) {
                bs.set(value);
            }
        });
        assertTrue(bs.allSet(0L, maxCapacity));
    }

    private long maxMemory() {
        File meminfo = new File("/proc/meminfo");
        try {
            try (Scanner sc = new Scanner(meminfo)) {
                sc.next();
                return sc.nextLong();
            }
        } catch (FileNotFoundException e) {
            return -1;
        }
    }
}
