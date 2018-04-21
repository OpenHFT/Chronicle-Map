/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.NullReturnValue;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ListenersTest {

    @Test
    public void testAnyRemove() {
        CountingEntryOperations<Integer, Integer> removeCounting =
                new CountingEntryOperations<>();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .entryOperations(removeCounting)
                        .create();

        map.put(1, 1);
        map.remove(1); // removeCount 1

        map.put(1, 1);
        assertFalse(map.remove(1, 2));
        map.remove(1, 1); // removeCount 2

        map.put(1, 1);
        map.merge(1, 1, (v1, v2) -> null); // removeCount 3

        map.put(1, 1);
        Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
        it.next();
        it.remove(); // removeCount 4

        assertEquals(4, removeCounting.removeCount.get());
    }

    @Test
    public void testAnyPut() {
        CountingEntryOperations<Integer, Integer> putCounting =
                new CountingEntryOperations<>();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .entryOperations(putCounting)
                        .create();

        map.put(1, 1); // insert 1

        map.put(1, 2); // replaceValue 1
        map.compute(1, (k, v) -> 2); // replaceValue 2

        map.entrySet().iterator().next().setValue(1); // replaceValue 3

        map.compute(2, (k, v) -> 1); // insert 2

        assertEquals(3, putCounting.replaceValueCount.get());
        assertEquals(2, putCounting.insertCount.get());
    }

    @Test
    public void testContainsKey() {
        AtomicInteger c = new AtomicInteger();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .mapMethods(new MapMethods<Integer, Integer, Void>() {
                            @Override
                            public boolean containsKey(MapQueryContext<Integer, Integer, Void> q) {
                                if (q.queriedKey().get() == 2)
                                    return false;
                                c.incrementAndGet();
                                return MapMethods.super.containsKey(q);
                            }
                        })
                        .create();

        assertFalse(map.containsKey(1)); // 1
        map.put(1, 1);
        assertTrue(map.containsKey(1)); // 2

        map.put(2, 2);
        assertFalse(map.containsKey(2));

        assertEquals(2, c.get());
    }

    @Test
    public void testGet() {
        AtomicInteger c = new AtomicInteger();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .mapMethods(new MapMethods<Integer, Integer, Void>() {
                            @Override
                            public void get(MapQueryContext<Integer, Integer, Void> q,
                                            ReturnValue<Integer> returnValue) {
                                if (q.queriedKey().get() == 2) {
                                    returnValue.returnValue(q.wrapValueAsData(42));
                                    return;
                                }
                                c.incrementAndGet();
                                MapMethods.super.get(q, returnValue);
                            }
                        })
                        .create();

        assertNull(map.get(1)); // 1
        map.put(1, 1);
        assertEquals(1, map.get(1).intValue()); // 2

        map.put(2, 2);
        assertEquals(42, map.get(2).intValue());

        assertEquals(2, c.get());
    }

    @Test
    public void testPut() {
        AtomicInteger c = new AtomicInteger();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .mapMethods(new MapMethods<Integer, Integer, Void>() {
                            @Override
                            public void put(MapQueryContext<Integer, Integer, Void> q,
                                            net.openhft.chronicle.hash.Data<Integer> value,
                                            ReturnValue<Integer> returnValue) {
                                if (q.queriedKey().get() == 2) {
                                    MapMethods.super.put(q, value, NullReturnValue.get());
                                    return;
                                }
                                if (q.queriedKey().get() == 3) {
                                    MapMethods.super.put(q, q.wrapValueAsData(value.get() + 1),
                                            returnValue);
                                    return;
                                }
                                c.incrementAndGet();
                                MapMethods.super.put(q, value, returnValue);
                            }
                        })
                        .create();

        assertNull(map.put(1, 1)); // 1
        assertEquals(1, map.put(1, 2).intValue()); // 2

        assertNull(map.put(2, 1));
        assertNull(map.put(2, 2));

        assertNull(map.put(3, 1));
        assertEquals(2, map.get(3).intValue());

        assertEquals(2, c.get());
    }

    static class CountingEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {
        AtomicInteger removeCount = new AtomicInteger();
        AtomicInteger insertCount = new AtomicInteger();
        AtomicInteger replaceValueCount = new AtomicInteger();

        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            removeCount.incrementAndGet();
            return MapEntryOperations.super.remove(entry);
        }

        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
            replaceValueCount.incrementAndGet();
            return MapEntryOperations.super.replaceValue(entry, newValue);
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
            insertCount.incrementAndGet();
            return MapEntryOperations.super.insert(absentEntry, value);
        }
    }
}
