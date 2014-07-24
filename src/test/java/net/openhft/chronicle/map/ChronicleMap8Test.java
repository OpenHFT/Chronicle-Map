package net.openhft.chronicle.map;/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*import junit.framework.Test;
import junit.framework.TestSuite;*/

import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.jrs166.JSR166TestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;

import static java.util.Spliterator.*;
import static org.junit.Assert.*;

public class ChronicleMap8Test extends JSR166TestCase {

    private static final double EPSILON = 1E-5;


    private static ChronicleMap<Long, Long> newLongMap() {
        //First create (or access if already created) the shared map

        final SharedHashMapBuilder<Long, Long> builder = SharedHashMapBuilder.of(Long.class, Long.class);


        //// don't include this, just to check it is as expected.
        assertEquals(8, builder.minSegments());
        //// end of test

        String shmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "SHMTest" + System.nanoTime();
        try {
            return builder.file(new File(shmPath)).create();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }


        // return new ChronicleMap(5);

    }

    private static ChronicleMap newIntegerStringMap() {


        final SharedHashMapBuilder<Integer, String> builder = SharedHashMapBuilder.of(Integer.class,
                String.class);


        //// don't include this, just to check it is as expected.
        assertEquals(8, builder.minSegments());
        //// end of test

        String shmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
                "SHMTest" + System.nanoTime();
        try {
            return builder.file(new File(shmPath)).create();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }


    }

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private static ChronicleMap map5() {
        ChronicleMap map = newIntegerStringMap();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        final boolean empty = map.isEmpty();
        assertFalse(empty);
        assertEquals(5, map.size());
        return map;
    }


    /**
     * getOrDefault returns value if present, else default
     */
    @Test
    public void testGetOrDefault() {
        ChronicleMap map = map5();
        assertEquals(map.getOrDefault(one, "Z"), "A");
        assertEquals(map.getOrDefault(six, "Z"), "Z");
    }

    /**
     * computeIfAbsent adds when the given key is not present
     */
    @Test
    public void testComputeIfAbsent() {
        ChronicleMap map = map5();
        map.computeIfAbsent(six, (x) -> "Z");
        assertTrue(map.containsKey(six));
    }

    /**
     * computeIfAbsent does not replace if the key is already present
     */
    @Test
    public void testComputeIfAbsent2() {
        ChronicleMap map = map5();
        assertEquals("A", map.computeIfAbsent(one, (x) -> "Z"));
    }

    /**
     * computeIfAbsent does not add if function returns null
     */
    @Test
    public void testComputeIfAbsent3() {
        ChronicleMap map = map5();
        map.computeIfAbsent(six, (x) -> null);
        assertFalse(map.containsKey(six));
    }

    /**
     * computeIfPresent does not replace if the key is already present
     */
    @Test
    public void testComputeIfPresent() {
        ChronicleMap map = map5();
        map.computeIfPresent(six, (x, y) -> "Z");
        assertFalse(map.containsKey(six));
    }

    /**
     * computeIfPresent adds when the given key is not present
     */
    @Test
    public void testComputeIfPresent2() {
        ChronicleMap map = map5();
        assertEquals("Z", map.computeIfPresent(one, (x, y) -> "Z"));
    }

    /**
     * compute does not replace if the function returns null
     */
    @Test
    public void testCompute() {
        ChronicleMap map = map5();
        map.compute(six, (x, y) -> null);
        assertFalse(map.containsKey(six));
    }

    /**
     * compute adds when the given key is not present
     */
    @Test
    public void testCompute2() {
        ChronicleMap map = map5();
        assertEquals("Z", map.compute(six, (x, y) -> "Z"));
    }

    /**
     * compute replaces when the given key is present
     */
    @Test
    public void testCompute3() {
        ChronicleMap map = map5();
        assertEquals("Z", map.compute(one, (x, y) -> "Z"));
    }

    /**
     * compute removes when the given key is present and function returns null
     */
    @Test
    public void testCompute4() {
        ChronicleMap map = map5();
        final BiFunction remappingFunction = (x, y) -> {
            return null;
        };
        map.compute(one, remappingFunction);
        assertFalse(map.containsKey(one));
    }

    /**
     * merge adds when the given key is not present
     */
    @Test
    public void testMerge1() {
        ChronicleMap map = map5();
        assertEquals("Y", map.merge(six, "Y", (x, y) -> "Z"));
    }

    /**
     * merge replaces when the given key is present
     */
    @Test
    public void testMerge2() {
        ChronicleMap map = map5();
        assertEquals("Z", map.merge(one, "Y", (x, y) -> "Z"));
    }

    /**
     * merge removes when the given key is present and function returns null
     */
    @Test
    public void testMerge3() {
        ChronicleMap map = map5();
        map.merge(one, "Y", (x, y) -> null);
        assertFalse(map.containsKey(one));
    }

    static Set<Integer> populatedSet(int n) {
        Set<Integer> a = ConcurrentHashMap.<Integer>newKeySet();
        assertTrue(a.isEmpty());
        for (int i = 0; i < n; i++)
            a.add(i);
        assertFalse(a.isEmpty());
        assertEquals(n, a.size());
        return a;
    }

    static Set populatedSet(Integer[] elements) {
        Set<Integer> a = ConcurrentHashMap.<Integer>newKeySet();
        assertTrue(a.isEmpty());
        for (int i = 0; i < elements.length; i++)
            a.add(elements[i]);
        assertFalse(a.isEmpty());
        assertEquals(elements.length, a.size());
        return a;
    }

    /*
     * replaceAll replaces all matching values.
     */
    @Test
    public void testReplaceAll() {
        ChronicleMap<Integer, String> map = map5();

        final BiFunction<Integer, String, String> function = (x, y) -> {
            return x > 3 ? "Z" : y;
        };

        map.replaceAll(function);
        assertEquals("A", map.get(one));
        assertEquals("B", map.get(two));
        assertEquals("C", map.get(three));
        assertEquals("Z", map.get(four));
        assertEquals("Z", map.get(five));
    }

    /**
     * Default-constructed set is empty
     */
    @Test
    public void testNewKeySet() {
        Set a = ConcurrentHashMap.newKeySet();
        assertTrue(a.isEmpty());
    }

    /**
     * keySet.add adds the key with the established value to the map; remove removes it.
     */
    @Test
    public void testKeySetAddRemove() {
        ChronicleMap map = map5();
        Set set1 = map.keySet();
        Set set2 = map.keySet(true);
        set2.add(six);
        assertTrue(((ConcurrentHashMap.KeySetView) set2).getMap() == map);
        assertTrue(((ConcurrentHashMap.KeySetView) set1).getMap() == map);
        assertEquals(set2.size(), map.size());
        assertEquals(set1.size(), map.size());
        assertTrue((Boolean) map.get(six));
        assertTrue(set1.contains(six));
        assertTrue(set2.contains(six));
        set2.remove(six);
        assertNull(map.get(six));
        assertFalse(set1.contains(six));
        assertFalse(set2.contains(six));
    }


    /**
     * keySet.addAll adds each element from the given collection
     */
    @Test
    public void testAddAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(three);
        v.add(four);
        v.add(five);
        full.addAll(v);
        assertEquals(6, full.size());
    }

    /**
     * keySet.addAll adds each element from the given collection that did not already exist in the set
     */
    @Test
    public void testAddAll2() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(three);
        v.add(four);
        v.add(one); // will not add this element
        full.addAll(v);
        assertEquals(5, full.size());
    }

    /**
     * keySet.add will not add the element if it already exists in the set
     */
    @Test
    public void testAdd2() {
        Set full = populatedSet(3);
        full.add(one);
        assertEquals(3, full.size());
    }

    /**
     * keySet.add adds the element when it does not exist in the set
     */
    @Test
    public void testAdd3() {
        Set full = populatedSet(3);
        full.add(three);
        assertTrue(full.contains(three));
    }

    /**
     * keySet.add throws UnsupportedOperationException if no default mapped value
     */
    @Test
    public void testAdd4() {
        Set full = map5().keySet();
        try {
            full.add(three);
            shouldThrow();
        } catch (UnsupportedOperationException e) {
        }
    }

    /**
     * keySet.add throws NullPointerException if the specified key is null
     */
    @Test
    public void testAdd5() {
        Set full = populatedSet(3);
        try {
            full.add(null);
            shouldThrow();
        } catch (NullPointerException e) {
        }
    }

    /**
     * KeySetView.getMappedValue returns the map's mapped value
     */
    @Test
    public void testGetMappedValue() {
        ChronicleMap map = map5();
        assertNull(map.keySet().getMappedValue());
        try {
            map.keySet(null);
            shouldThrow();
        } catch (NullPointerException e) {
        }

        final net.openhft.collections.KeySetView  set = map.keySet(one);

        set.add(one);
        set.add(six);
        set.add(seven);
        assertTrue(set.getMappedValue() == one);
        assertTrue(map.get(one) != one);
        assertTrue(map.get(six) == one);
        assertTrue(map.get(seven) == one);
    }

    void checkSpliteratorCharacteristics(Spliterator<?> sp,
                                         int requiredCharacteristics) {
        assertEquals(requiredCharacteristics,
                requiredCharacteristics & sp.characteristics());
    }

    /**
     * KeySetView.spliterator returns spliterator over the elements in this set
     */
    @Test
    public void testKeySetSpliterator() {
        LongAdder adder = new LongAdder();
        ChronicleMap map = map5();
        Set set = map.keySet();
        Spliterator<Integer> sp = set.spliterator();
        checkSpliteratorCharacteristics(sp, CONCURRENT | DISTINCT | NONNULL);
        assertEquals(sp.estimateSize(), map.size());
        Spliterator<Integer> sp2 = sp.trySplit();
        sp.forEachRemaining((Integer x) -> adder.add(x.longValue()));
        long v = adder.sumThenReset();
        sp2.forEachRemaining((Integer x) -> adder.add(x.longValue()));
        long v2 = adder.sum();
        assertEquals(v + v2, 15);
    }

    /**
     * keyset.clear removes all elements from the set
     */
    @Test
    public void testClear() {
        Set full = populatedSet(3);
        full.clear();
        assertEquals(0, full.size());
    }

    /**
     * keyset.contains returns true for added elements
     */
    @Test
    public void testContains() {
        Set full = populatedSet(3);
        assertTrue(full.contains(one));
        assertFalse(full.contains(five));
    }

    /**
     * KeySets with equal elements are equal
     */
    @Test
    public void testEquals() {
        Set a = populatedSet(3);
        Set b = populatedSet(3);
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
        assertEquals(a.hashCode(), b.hashCode());
        a.add(m1);
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        b.add(m1);
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
        assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * KeySet.containsAll returns true for collections with subset of elements
     */
    @Test
    public void testContainsAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(one);
        v.add(two);
        assertTrue(full.containsAll(v));
        v.add(six);
        assertFalse(full.containsAll(v));
    }

    /**
     * KeySet.isEmpty is true when empty, else false
     */
    @Test
    public void testIsEmpty() {
        Set empty = ConcurrentHashMap.newKeySet();
        Set full = populatedSet(3);
        assertTrue(empty.isEmpty());
        assertFalse(full.isEmpty());
    }

    /**
     * KeySet.iterator() returns an iterator containing the elements of the set
     */
    @Test
    public void testIterator() {
        Collection empty = ConcurrentHashMap.newKeySet();
        int size = 20;
        assertFalse(empty.iterator().hasNext());
        try {
            empty.iterator().next();
            shouldThrow();
        } catch (NoSuchElementException success) {
        }

        Integer[] elements = new Integer[size];
        for (int i = 0; i < size; i++)
            elements[i] = i;
        Collections.shuffle(Arrays.asList(elements));
        Collection<Integer> full = populatedSet(elements);

        Iterator it = full.iterator();
        for (int j = 0; j < size; j++) {
            assertTrue(it.hasNext());
            it.next();
        }
        assertFalse(it.hasNext());
        try {
            it.next();
            shouldThrow();
        } catch (NoSuchElementException success) {
        }
    }

    /**
     * KeySet.iterator.remove removes current element
     */
    @Test
    public void testIteratorRemove() {
        Set q = populatedSet(3);
        Iterator it = q.iterator();
        Object removed = it.next();
        it.remove();

        it = q.iterator();
        assertFalse(it.next().equals(removed));
        assertFalse(it.next().equals(removed));
        assertFalse(it.hasNext());
    }

    /**
     * KeySet.toString holds toString of elements
     */
    @Test
    public void testToString() {
        assertEquals("[]", ConcurrentHashMap.newKeySet().toString());
        Set full = populatedSet(3);
        String s = full.toString();
        for (int i = 0; i < 3; ++i)
            assertTrue(s.contains(String.valueOf(i)));
    }

    /**
     * KeySet.removeAll removes all elements from the given collection
     */
    @Test
    public void testRemoveAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(one);
        v.add(two);
        full.removeAll(v);
        assertEquals(1, full.size());
    }

    /**
     * KeySet.remove removes an element
     */
    @Test
    public void testRemove() {
        Set full = populatedSet(3);
        full.remove(one);
        assertFalse(full.contains(one));
        assertEquals(2, full.size());
    }

    /**
     * keySet.size returns the number of elements
     */
    @Test
    public void testSize() {
        Set empty = ConcurrentHashMap.newKeySet();
        Set full = populatedSet(3);
        assertEquals(3, full.size());
        assertEquals(0, empty.size());
    }

    /**
     * KeySet.toArray() returns an Object array containing all elements from the set
     */
    @Test
    public void testToArray() {
        Object[] a = ConcurrentHashMap.newKeySet().toArray();
        assertTrue(Arrays.equals(new Object[0], a));
        assertSame(Object[].class, a.getClass());
        int size = 20;
        Integer[] elements = new Integer[size];
        for (int i = 0; i < size; i++)
            elements[i] = i;
        Collections.shuffle(Arrays.asList(elements));
        Collection<Integer> full = populatedSet(elements);

        assertTrue(Arrays.asList(elements).containsAll(Arrays.asList(full.toArray())));
        assertTrue(full.containsAll(Arrays.asList(full.toArray())));
        assertSame(Object[].class, full.toArray().getClass());
    }

    /**
     * toArray(Integer array) returns an Integer array containing all elements from the set
     */
    @Test
    public void testToArray2() {
        Collection empty = ConcurrentHashMap.newKeySet();
        Integer[] a;
        int size = 20;

        a = new Integer[0];
        assertSame(a, empty.toArray(a));

        a = new Integer[size / 2];
        Arrays.fill(a, 42);
        assertSame(a, empty.toArray(a));
        assertNull(a[0]);
        for (int i = 1; i < a.length; i++)
            assertEquals(42, (int) a[i]);

        Integer[] elements = new Integer[size];
        for (int i = 0; i < size; i++)
            elements[i] = i;
        Collections.shuffle(Arrays.asList(elements));
        Collection<Integer> full = populatedSet(elements);

        Arrays.fill(a, 42);
        assertTrue(Arrays.asList(elements).containsAll(Arrays.asList(full.toArray(a))));
        for (int i = 0; i < a.length; i++)
            assertEquals(42, (int) a[i]);
        assertSame(Integer[].class, full.toArray(a).getClass());

        a = new Integer[size];
        Arrays.fill(a, 42);
        assertSame(a, full.toArray(a));
        assertTrue(Arrays.asList(elements).containsAll(Arrays.asList(full.toArray(a))));
    }

    /**
     * A deserialized serialized set is equal
     */
    @Test
    public void testSerialization() throws Exception {
        int size = 20;
        Set x = populatedSet(size);
        Set y = serialClone(x);

        assertNotSame(x, y);
        assertEquals(x.size(), y.size());
        assertEquals(x, y);
        assertEquals(y, x);
    }

    static final int SIZE = 10000;
    static ChronicleMap<Long, Long> longMap;

    static ChronicleMap<Long, Long> longMap() {
        if (longMap == null) {
            longMap = newLongMap();
            for (int i = 0; i < SIZE; ++i)
                longMap.put(Long.valueOf(i), Long.valueOf(2 * i));
        }

        return longMap;
    }


    // explicit function class to avoid type inference problems
    static class AddKeys implements BiFunction<Map.Entry<Long, Long>, Map.Entry<Long, Long>, Map.Entry<Long, Long>> {
        public Map.Entry<Long, Long> apply(Map.Entry<Long, Long> x, Map.Entry<Long, Long> y) {
            return new AbstractMap.SimpleEntry<Long, Long>
                    (Long.valueOf(x.getKey().longValue() + y.getKey().longValue()),
                            Long.valueOf(1L));
        }
    }

    /**
     * forEachKeySequentially traverses all keys
     */
    @Test
    public void testForEachKeySequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Consumer<Long> consumer = (Long x) -> adder.add(x.longValue());
        m.forEachKey(Long.MAX_VALUE, consumer);
        assertEquals(adder.sum(), SIZE * (SIZE - 1) / 2);
    }

    /**
     * forEachValueSequentially traverses all values
     */
    @Test
    public void testForEachValueSequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = (ChronicleMap) longMap();
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachValue(Long.MAX_VALUE, action);
        assertEquals(adder.sum(), SIZE * (SIZE - 1));
    }

    /**
     * forEachSequentially traverses all mappings
     */
    @Test
    public void testForEachSequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final BiConsumer<Long, Long> action = (Long x, Long y) -> adder.add(x.longValue() + y.longValue());
        m.forEach(Long.MAX_VALUE, action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * forEachEntrySequentially traverses all entries
     */
    @Test
    public void testForEachEntrySequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Consumer<Map.Entry<Long, Long>> action = (Map.Entry<Long, Long> e) -> adder.add(e.getKey().longValue() + e.getValue().longValue());
        m.forEachEntry(Long.MAX_VALUE, action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * forEachKeyInParallel traverses all keys
     */
    @Test
    public void testForEachKeyInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());

        m.forEachKey(1L, action);
        assertEquals(adder.sum(), SIZE * (SIZE - 1) / 2);
    }

    /**
     * forEachValueInParallel traverses all values
     */
    @Test
    public void testForEachValueInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachValue(1L, action);
        assertEquals(adder.sum(), SIZE * (SIZE - 1));
    }

    /**
     * forEachInParallel traverses all mappings
     */
    @Test
    public void testForEachInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final BiConsumer<Long, Long> action = (Long x, Long y) -> adder.add(x.longValue() + y.longValue());
        m.forEach(1L, action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * forEachEntryInParallel traverses all entries
     */
    @Test
    public void testForEachEntryInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Consumer<Map.Entry<Long, Long>> action = (Map.Entry<Long, Long> e) -> adder.add(e.getKey().longValue() + e.getValue().longValue());
        m.forEachEntry(1L, action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachKeySequentially traverses the given transformations of all keys
     */
    @Test
    public void testMappedForEachKeySequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachKey(Long.MAX_VALUE, transformer,
                action);
        assertEquals(adder.sum(), 4 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachValueSequentially traverses the given transformations of all values
     */
    @Test
    public void testMappedForEachValueSequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachValue(Long.MAX_VALUE, transformer,
                action);
        assertEquals(adder.sum(), 4 * SIZE * (SIZE - 1));
    }

    /**
     * Mapped forEachSequentially traverses the given transformations of all mappings
     */
    @Test
    public void testMappedForEachSequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final BiFunction<Long, Long, Long> transformer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEach(Long.MAX_VALUE, transformer,
                action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachEntrySequentially traverses the given transformations of all entries
     */
    @Test
    public void testMappedForEachEntrySequentially() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Map.Entry<Long, Long>, Long> transformer = (Map.Entry<Long, Long> e) -> Long.valueOf(e.getKey().longValue() + e.getValue().longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachEntry(Long.MAX_VALUE, transformer,
                action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachKeyInParallel traverses the given transformations of all keys
     */
    @Test
    public void testMappedForEachKeyInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachKey(1L, transformer,
                action);
        assertEquals(adder.sum(), 4 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachValueInParallel traverses the given transformations of all values
     */
    @Test
    public void testMappedForEachValueInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachValue(1L, transformer,
                action);
        assertEquals(adder.sum(), 4 * SIZE * (SIZE - 1));
    }

    /**
     * Mapped forEachInParallel traverses the given transformations of all mappings
     */
    @Test
    public void testMappedForEachInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final BiFunction<Long, Long, Long> transformer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEach(1L, transformer,
                action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped forEachEntryInParallel traverses the given transformations of all entries
     */
    @Test
    public void testMappedForEachEntryInParallel() {
        LongAdder adder = new LongAdder();
        ChronicleMap<Long, Long> m = longMap();
        final Function<Map.Entry<Long, Long>, Long> transformer = (Map.Entry<Long, Long> e) -> Long.valueOf(e.getKey().longValue() + e.getValue().longValue());
        final Consumer<Long> action = (Long x) -> adder.add(x.longValue());
        m.forEachEntry(1L, transformer,
                action);
        assertEquals(adder.sum(), 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysSequentially accumulates across all keys,
     */
    @Test
    public void testReduceKeysSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        r = m.reduceKeys(Long.MAX_VALUE, reducer);
        assertEquals((long) r, (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceValuesSequentially accumulates across all values
     */
    @Test
    public void testReduceValuesSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        r = m.reduceKeys(Long.MAX_VALUE, reducer);
        assertEquals((long) r, (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceEntriesSequentially accumulates across all entries
     */
    @Test
    public void testReduceEntriesSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Map.Entry<Long, Long> r;
        final AddKeys reducer = new AddKeys();
        r = m.reduceEntries(Long.MAX_VALUE, reducer);
        assertEquals(r.getKey().longValue(), (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysInParallel accumulates across all keys
     */
    @Test
    public void testReduceKeysInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        r = m.reduceKeys(1L, reducer);
        assertEquals((long) r, (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceValuesInParallel accumulates across all values
     */
    @Test
    public void testReduceValuesInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        r = m.reduceValues(1L, reducer);
        assertEquals((long) r, (long) SIZE * (SIZE - 1));
    }

    /**
     * reduceEntriesInParallel accumulate across all entries
     */
    @Test
    public void testReduceEntriesInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Map.Entry<Long, Long> r;
        r = m.reduceEntries(1L, new AddKeys());
        assertEquals(r.getKey().longValue(), (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped reduceKeysSequentially accumulates mapped keys
     */
    @Test
    public void testMapReduceKeysSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        Long r = m.reduceKeys(Long.MAX_VALUE, transformer,
                reducer);
        assertEquals((long) r, (long) 4 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped reduceValuesSequentially accumulates mapped values
     */
    @Test
    public void testMapReduceValuesSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        Long r = m.reduceValues(Long.MAX_VALUE, transformer,
                reducer);
        assertEquals((long) r, (long) 4 * SIZE * (SIZE - 1));
    }

    /**
     * reduceSequentially accumulates across all transformed mappings
     */
    @Test
    public void testMappedReduceSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final BiFunction<Long, Long, Long> transformer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        Long r = m.reduce(Long.MAX_VALUE, transformer,
                reducer);

        assertEquals((long) r, (long) 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped reduceKeysInParallel, accumulates mapped keys
     */
    @Test
    public void testMapReduceKeysInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        Long r = m.reduceKeys(1L, transformer,
                reducer);
        assertEquals((long) r, (long) 4 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * Mapped reduceValuesInParallel accumulates mapped values
     */
    @Test
    public void testMapReduceValuesInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final Function<Long, Long> transformer = (Long x) -> Long.valueOf(4 * x.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        Long r = m.reduceValues(1L, transformer,
                reducer);
        assertEquals((long) r, (long) 4 * SIZE * (SIZE - 1));
    }

    /**
     * reduceInParallel accumulate across all transformed mappings
     */
    @Test
    public void testMappedReduceInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> transformer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        final BiFunction<Long, Long, Long> reducer = (Long x, Long y) -> Long.valueOf(x.longValue() + y.longValue());
        r = m.reduce(1L, transformer,
                reducer);
        assertEquals((long) r, (long) 3 * SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysToLongSequentially accumulates mapped keys
     */
    @Test
    public void testReduceKeysToLongSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToLongFunction<Long> transformer = (Long x) -> x.longValue();
        final LongBinaryOperator sum = Long::sum;
        long lr = m.reduceKeysToLong(Long.MAX_VALUE, transformer, 0L, sum);
        assertEquals(lr, (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysToIntSequentially accumulates mapped keys
     */
    @Test
    public void testReduceKeysToIntSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToIntFunction<Long> transformer = (Long x) -> x.intValue();
        final IntBinaryOperator sum = Integer::sum;
        int ir = m.reduceKeysToInt(Long.MAX_VALUE, transformer, 0, sum);
        assertEquals(ir, SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysToDoubleSequentially accumulates mapped keys
     */
    @Test
    public void testReduceKeysToDoubleSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToDoubleFunction<Long> transformer = (Long x) -> x.doubleValue();
        double dr = m.reduceKeysToDouble(Long.MAX_VALUE, transformer, 0.0, Double::sum);
        assertEquals(dr, (double) SIZE * (SIZE - 1) / 2, EPSILON);
    }

    /**
     * reduceValuesToLongSequentially accumulates mapped values
     */
    @Test
    public void testReduceValuesToLongSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToLongFunction<Long> transformer = (Long x) -> x.longValue();
        long lr = m.reduceValuesToLong(Long.MAX_VALUE, transformer, 0L, Long::sum);
        assertEquals(lr, (long) SIZE * (SIZE - 1));
    }

    /**
     * reduceValuesToIntSequentially accumulates mapped values
     */
    @Test
    public void testReduceValuesToIntSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToIntFunction<Long> transformer = (Long x) -> x.intValue();
        int ir = m.reduceValuesToInt(Long.MAX_VALUE, transformer, 0, Integer::sum);
        assertEquals(ir, SIZE * (SIZE - 1));
    }

    /**
     * reduceValuesToDoubleSequentially accumulates mapped values
     */
    @Test
    public void testReduceValuesToDoubleSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        final ToDoubleFunction<Long> transformer = (Long x) -> x.doubleValue();
        double dr = m.reduceValuesToDouble(Long.MAX_VALUE, transformer, 0.0, Double::sum);
        assertEquals(dr, (double) SIZE * (SIZE - 1), EPSILON);
    }

    /**
     * reduceKeysToLongInParallel accumulates mapped keys
     */
    @Test
    public void testReduceKeysToLongInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToLongFunction<Long> transformer = (Long x) -> x.longValue();
        long lr = m.reduceKeysToLong(1L, transformer, 0L, Long::sum);
        assertEquals(lr, (long) SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysToIntInParallel accumulates mapped keys
     */
    @Test
    public void testReduceKeysToIntInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToIntFunction<Long> transformer = (Long x) -> x.intValue();
        int ir = m.reduceKeysToInt(1L, transformer, 0, Integer::sum);
        assertEquals(ir, SIZE * (SIZE - 1) / 2);
    }

    /**
     * reduceKeysToDoubleInParallel accumulates mapped values
     */
    @Test
    public void testReduceKeysToDoubleInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToDoubleFunction<Long> transformer = (Long x) -> x.doubleValue();
        double dr = m.reduceKeysToDouble(1L, transformer, 0.0, Double::sum);
        assertEquals(dr, (double) SIZE * (SIZE - 1) / 2, 0.01);
    }

    /**
     * reduceValuesToLongInParallel accumulates mapped values
     */
    @Test
    public void testReduceValuesToLongInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToLongFunction<Long> transformer = (Long x) -> x.longValue();
        long lr = m.reduceValuesToLong(1L, transformer, 0L, Long::sum);
        assertEquals(lr, (long) SIZE * (SIZE - 1));
    }

    /**
     * reduceValuesToIntInParallel accumulates mapped values
     */
    @Test
    public void testReduceValuesToIntInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToIntFunction<Long> transformer = (Long x) -> x.intValue();
        int ir = m.reduceValuesToInt(1L, transformer, 0, Integer::sum);
        assertEquals(ir, SIZE * (SIZE - 1));
    }

    /**
     * reduceValuesToDoubleInParallel accumulates mapped values
     */
    @Test
    public void testReduceValuesToDoubleInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        final ToDoubleFunction<Long> transformer = (Long x) -> x.doubleValue();
        double dr = m.reduceValuesToDouble(1L, transformer, 0.0, Double::sum);
        assertEquals(dr, (double) SIZE * (SIZE - 1), EPSILON);
    }

    /**
     * searchKeysSequentially returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchKeysSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Long, Long> searchFunction = (Long x) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.searchKeys(Long.MAX_VALUE, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Long, Long> searchFunction1 = (Long x) -> x.longValue() < 0L ? x : null;
        r = m.searchKeys(Long.MAX_VALUE, searchFunction1);
        assertNull(r);
    }

    /**
     * searchValuesSequentially returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchValuesSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Long, Long> searchFunction = (Long x) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.searchValues(Long.MAX_VALUE, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Long, Long> searchFunction1 = (Long x) -> x.longValue() < 0L ? x : null;
        r = m.searchValues(Long.MAX_VALUE, searchFunction1);
        assertNull(r);
    }

    /**
     * searchSequentially returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> searchFunction = (Long x, Long y) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.search(Long.MAX_VALUE, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final BiFunction<Long, Long, Long> searchFunction1 = (Long x, Long y) -> x.longValue() < 0L ? x : null;
        r = m.search(Long.MAX_VALUE, searchFunction1);
        assertNull(r);
    }

    /**
     * searchEntriesSequentially returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchEntriesSequentially() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Map.Entry<Long, Long>, Long> searchFunction = (Map.Entry<Long, Long> e) -> e.getKey().longValue() == (long) (SIZE / 2) ? e.getKey() : null;
        r = m.searchEntries(Long.MAX_VALUE, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Map.Entry<Long, Long>, Long> searchFunction1 = (Map.Entry<Long, Long> e) -> e.getKey().longValue() < 0L ? e.getKey() : null;
        r = m.searchEntries(Long.MAX_VALUE, searchFunction1);
        assertNull(r);
    }

    /**
     * searchKeysInParallel returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchKeysInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Long, Long> searchFunction = (Long x) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.searchKeys(1L, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Long, Long> searchFunction1 = (Long x) -> x.longValue() < 0L ? x : null;
        r = m.searchKeys(1L, searchFunction1);
        assertNull(r);
    }

    /**
     * searchValuesInParallel returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchValuesInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Long, Long> searchFunction = (Long x) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.searchValues(1L, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Long, Long> searchFunction1 = (Long x) -> x.longValue() < 0L ? x : null;
        r = m.searchValues(1L, searchFunction1);
        assertNull(r);
    }

    /**
     * searchInParallel returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final BiFunction<Long, Long, Long> searchFunction = (Long x, Long y) -> x.longValue() == (long) (SIZE / 2) ? x : null;
        r = m.search(1L, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final BiFunction<Long, Long, Long> searchFunction1 = (Long x, Long y) -> x.longValue() < 0L ? x : null;
        r = m.search(1L, searchFunction1);
        assertNull(r);
    }

    /**
     * searchEntriesInParallel returns a non-null result of search function, or null if none
     */
    @Test
    public void testSearchEntriesInParallel() {
        ChronicleMap<Long, Long> m = longMap();
        Long r;
        final Function<Map.Entry<Long, Long>, Long> searchFunction = (Map.Entry<Long, Long> e) -> e.getKey().longValue() == (long) (SIZE / 2) ? e.getKey() : null;
        r = m.searchEntries(1L, searchFunction);
        assertEquals((long) r, (long) (SIZE / 2));
        final Function<Map.Entry<Long, Long>, Long> searchFunction1 = (Map.Entry<Long, Long> e) -> e.getKey().longValue() < 0L ? e.getKey() : null;
        r = m.searchEntries(1L, searchFunction1);
        assertNull(r);
    }

}