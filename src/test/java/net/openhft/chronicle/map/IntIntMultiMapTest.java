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


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: peter Date: 09/12/13
 */
@RunWith(value = Parameterized.class)
public class IntIntMultiMapTest {

    IntIntMultiMap map;
    Multimap<Integer, Integer> referenceMap = HashMultimap.create();
    private Class<? extends IntIntMultiMap> c;

    public IntIntMultiMapTest(Class<? extends IntIntMultiMap> c)
            throws Exception {
        this.c = c;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {VanillaIntIntMultiMap.class},
                {VanillaShortShortMultiMap.class}
        });
    }

    private void initMap(int capacity) {
        try {
            map = c.getConstructor(int.class).newInstance(capacity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void multiMapEquals() {
        class Action implements IntIntMultiMap.EntryConsumer {
            int mapSize = 0;

            @Override
            public void accept(int key, int value) {
                mapSize++;
                assertTrue(referenceMap.containsEntry(key, value));
            }
        }
        Action action = new Action();
        map.forEach(action);
        assertEquals(referenceMap.size(), action.mapSize);
    }

    private void valuesEqualsByKey(int k) {
        List<Integer> values = new ArrayList<Integer>();
        map.startSearch(k);
        int v;
        while ((v = map.nextPos()) >= 0)
            values.add(v);
        Set<Integer> valueSet = new HashSet<Integer>(values);
        assertEquals(values.size(), valueSet.size());
        assertEquals(new HashSet<Integer>(referenceMap.get(k)), valueSet);
    }

    private void put(int k, int v) {
        map.put(k, v);
        referenceMap.put(k, v);
    }

    private void remove(int k, int v, boolean present) {
        assertEquals(present, map.remove(k, v));
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

        remove(1, 11, true);
        multiMapEquals();
        remove(1, 11, false);

        valuesEqualsByKey(3);
        valuesEqualsByKey(1);

        remove(1, 12, true);
        multiMapEquals();

        remove(1, 15, true);
        multiMapEquals();

        remove(1, 13, true);
        multiMapEquals();

        remove(1, 14, true);
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
}
