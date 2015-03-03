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
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.impl.hashlookup.HashLookup;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static net.openhft.lang.io.NativeBytes.UNSAFE;

/**
 * User: peter.lawrey Date: 09/12/13
 */
@RunWith(value = Parameterized.class)
public class HashLookupTest {

    static final long CAPACITY = 64;
    static long address;
    HashLookup map = new HashLookup();
    Multimap<Long, Long> referenceMap = HashMultimap.create();

    public HashLookupTest(Integer entrySize, Integer keyBits, Integer valueBits)
            throws Exception {
        map.reuse(address, CAPACITY, entrySize, keyBits, valueBits);
        clean();
    }

    void clean() {
        UNSAFE.setMemory(address, CAPACITY * 8L, (byte) 0);
    }

    @BeforeClass
    public static void setUp() {
        address = UNSAFE.allocateMemory(CAPACITY * 8L);
    }

    @AfterClass
    public static void tearDown() {
        UNSAFE.freeMemory(address);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {3, 12, 9},
                {4, 18, 14},
                {8, 33, 29},
        });
    }

    private void multiMapEquals() {
        class Action implements EntryConsumer {
            int mapSize = 0;

            @Override
            public void accept(long key, long value) {
                mapSize++;
                Assert.assertTrue(referenceMap.containsEntry(key, value));
            }
        }
        Action action = new Action();
        map.forEach(action);
        Assert.assertEquals(referenceMap.size(), action.mapSize);
    }

    private void valuesEqualsByKey(long k) {
        List<Long> values = new ArrayList<>();
        map.init0(k);
        map.initSearch0();
        long v;
        while ((v = map.nextPos()) >= 0L)
            values.add(v);
        Set<Long> valueSet = new HashSet<Long>(values);
        Assert.assertEquals(values.size(), valueSet.size());
        Assert.assertEquals(new HashSet<>(referenceMap.get(k)), valueSet);
    }

    private void put(long k, long v) {
        put0(k, v);
        referenceMap.put(k, v);
    }

    private void put0(long k, long v) {
        map.init0(k);
        map.initSearch0();
        while (map.nextPos() >= 0L)
            ;
        map.put(v);
        map.closeSearch0();
        map.close0();
    }

    private void remove(long k, long value) {
        long v = remove0(k, value);
        referenceMap.remove(k, v);
    }

    private long remove0(long k, long value) {
        map.init0(k);
        map.initSearch0();
        long v;
        while ((v = map.nextPos()) >= 0L && v != value)
            ;
        map.found();
        map.remove();
        map.closeSearch0();
        map.close0();
        return v;
    }

    @Test
    public void testPutRemoveSearch() {
        clean();
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
        clean();

        put0(15, 1);
        remove0(15, 1);
        map.init0(15);
        map.initSearch0();
        Assert.assertTrue(map.nextPos() < 0);
    }
}
