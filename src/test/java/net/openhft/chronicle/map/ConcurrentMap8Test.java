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

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*import junit.framework.Test;
import junit.framework.TestSuite;*/

import net.openhft.chronicle.map.jsr166.JSR166TestCase;

import java.util.concurrent.ConcurrentMap;

public class ConcurrentMap8Test extends JSR166TestCase {

    static final int SIZE = 10000;
    private static final double EPSILON = 1E-5;
    static ConcurrentMap<Long, Long> longMap;

    // ******** commented out for the moment as it only works in java8     ********************************

   /* private static ConcurrentMap<Long, Long> newLongMap() {
        //First create (or access if already created) the shared map

        final ChronicleMapOnHeapUpdatableBuilder<Long, Long> builder = ChronicleMapBuilder.of(Long.class, Long.class);

        //// don't include this, just to check it is as expected.
        assertEquals(8, builder.minSegments());
        //// end of test

        String chmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "CHMTest" + System.nanoTime();
        try {
            return builder.create(new File(chmPath));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // return new ConcurrentMap(5);
    }

    private static ConcurrentMap newIntegerStringMap() {
        final ChronicleMapOnHeapUpdatableBuilder<Integer, String> builder = ChronicleMapBuilder.of(Integer.class,
                String.class);

        //// don't include this, just to check it is as expected.
        assertEquals(8, builder.minSegments());
        //// end of test

        String chmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
                "CHMTest" + System.nanoTime();
        try {
            return builder.create(new File(chmPath));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    *//**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     *//*
    private static ConcurrentMap map5() {
        ConcurrentMap map = newIntegerStringMap();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        System.out.println(map.toString());
        final boolean empty = map.isEmpty();
        assertFalse(empty);
        assertEquals(5, map.size());
        return map;
    }

    *//**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     *//*
    private static ConcurrentHashMap map5ConcurrentHashMap() {
        ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<Integer, String>(5);
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        System.out.println(map.toString());
        final boolean empty = map.isEmpty();
        assertFalse(empty);
        assertEquals(5, map.size());
        return map;
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

    static ConcurrentMap<Long, Long> longMap() {
        if (longMap == null) {
            longMap = newLongMap();
            for (int i = 0; i < size; ++i)
                longMap.put(Long.valueOf(i), Long.valueOf(2 * i));
        }

        return longMap;
    }

    *//**
     * getOrDefault returns value if present, else default
     *//*
    @Test
    public void testGetOrDefault() {
        ConcurrentMap map = map5();
        assertEquals(map.getOrDefault(one, "Z"), "A");
        assertEquals(map.getOrDefault(six, "Z"), "Z");
    }

    *//**
     * computeIfAbsent adds when the given key is not present
     *//*
    @Test
    public void testComputeIfAbsent() {
        ConcurrentMap map = map5();
        map.computeIfAbsent(six, (x) -> "Z");
        assertTrue(map.containsKey(six));
    }

    *//**
     * computeIfAbsent does not replace if the key is already present
     *//*
    @Test
    public void testComputeIfAbsent2() {
        ConcurrentMap map = map5();
        assertEquals("A", map.computeIfAbsent(one, (x) -> "Z"));
    }

    *//**
     * computeIfAbsent does not add if function returns null
     *//*
    @Test
    public void testComputeIfAbsent3() {
        ConcurrentMap map = map5();
        map.computeIfAbsent(six, (x) -> null);
        assertFalse(map.containsKey(six));
    }

    *//**
     * computeIfPresent does not replace if the key is already present
     *//*
    @Test
    public void testComputeIfPresent() {
        ConcurrentMap map = map5();
        map.computeIfPresent(six, (x, y) -> "Z");
        assertFalse(map.containsKey(six));
    }

    *//**
     * computeIfPresent adds when the given key is not present
     *//*
    @Test
    public void testComputeIfPresent2() {
        ConcurrentMap map = map5();
        assertEquals("Z", map.computeIfPresent(one, (x, y) -> "Z"));
    }

    *//**
     * compute does not replace if the function returns null
     *//*
    @Test
    public void testCompute() {
        ConcurrentMap map = map5();
        map.compute(six, (x, y) -> null);
        assertFalse(map.containsKey(six));
    }

    *//**
     * compute adds when the given key is not present
     *//*
    @Test
    public void testCompute2() {
        ConcurrentMap map = map5();
        assertEquals("Z", map.compute(six, (x, y) -> "Z"));
    }

    *//**
     * compute replaces when the given key is present
     *//*
    @Test
    public void testCompute3() {
        ConcurrentMap map = map5();
        assertEquals("Z", map.compute(one, (x, y) -> "Z"));
    }

    *//**
     * compute removes when the given key is present and function returns null
     *//*
    @Test
    public void testCompute4() {
        ConcurrentMap map = map5();
        final BiFunction remappingFunction = (x, y) -> {
            return null;
        };
        map.compute(one, remappingFunction);
        assertFalse(map.containsKey(one));
    }

    *//**
     * merge adds when the given key is not present
     *//*
    @Test
    public void testMerge1() {
        ConcurrentMap map = map5();
        assertEquals("Y", map.merge(six, "Y", (x, y) -> "Z"));
    }

    *//**
     * merge replaces when the given key is present
     *//*
    @Test
    public void testMerge2() {
        ConcurrentMap map = map5();
        assertEquals("Z", map.merge(one, "Y", (x, y) -> "Z"));
    }

    *//**
     * merge removes when the given key is present and function returns null
     *//*
    @Test
    public void testMerge3() {
        ConcurrentMap map = map5();
        map.merge(one, "Y", (x, y) -> null);
        assertFalse(map.containsKey(one));
    }

    *//*
     * replaceAll replaces all matching values.
     *//*
    @Test
    public void testReplaceAll() {
        ConcurrentMap<Integer, String> map = map5();

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

    *//**
     * Default-constructed set is empty
     *//*
    @Test
    public void testNewKeySet() {
        Set a = ConcurrentHashMap.newKeySet();
        assertTrue(a.isEmpty());
    }

    *//**
     * keySet.addAll adds each element from the given collection
     *//*
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

    *//**
     * keySet.addAll adds each element from the given collection that did not already exist in the set
     *//*
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

    *//**
     * keySet.add will not add the element if it already exists in the set
     *//*
    @Test
    public void testAdd2() {
        Set full = populatedSet(3);
        full.add(one);
        assertEquals(3, full.size());
    }

    *//**
     * keySet.add adds the element when it does not exist in the set
     *//*
    @Test
    public void testAdd3() {
        Set full = populatedSet(3);
        full.add(three);
        assertTrue(full.contains(three));
    }

    *//**
     * keySet.add throws UnsupportedOperationException if no default mapped value
     *//*
    @Test
    public void testAdd4() {
        Set full = map5().keySet();
        try {
            full.add(three);
            shouldThrow();
        } catch (UnsupportedOperationException e) {
        }
    }

    *//**
     * keySet.add throws NullPointerException if the specified key is null
     *//*
    @Test
    public void testAdd5() {
        Set full = populatedSet(3);
        try {
            full.add(null);
            shouldThrow();
        } catch (NullPointerException e) {
        }
    }

    *//**
     * keyset.clear removes all elements from the set
     *//*
    @Test
    public void testClear() {
        Set full = populatedSet(3);
        full.clear();
        assertEquals(0, full.size());
    }

    *//**
     * keyset.contains returns true for added elements
     *//*
    @Test
    public void testContains() {
        Set full = populatedSet(3);
        assertTrue(full.contains(one));
        assertFalse(full.contains(five));
    }

    *//**
     * KeySets with equal elements are equal
     *//*
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

    *//**
     * KeySet.containsAll returns true for collections with subset of elements
     *//*
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

    *//**
     * KeySet.isEmpty is true when empty, else false
     *//*
    @Test
    public void testIsEmpty() {
        Set empty = ConcurrentHashMap.newKeySet();
        Set full = populatedSet(3);
        assertTrue(empty.isEmpty());
        assertFalse(full.isEmpty());
    }

    *//**
     * KeySet.iterator() returns an iterator containing the elements of the set
     *//*
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

    *//**
     * KeySet.iterator.remove removes current element
     *//*
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

    *//**
     * KeySet.toString holds toString of elements
     *//*
    @Test
    public void testToString() {
        assertEquals("[]", ConcurrentHashMap.newKeySet().toString());
        Set full = populatedSet(3);
        String s = full.toString();
        for (int i = 0; i < 3; ++i)
            assertTrue(s.contains(String.valueOf(i)));
    }

    *//**
     * KeySet.removeAll removes all elements from the given collection
     *//*
    @Test
    public void testRemoveAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(one);
        v.add(two);
        full.removeAll(v);
        assertEquals(1, full.size());
    }

    *//**
     * KeySet.remove removes an element
     *//*
    @Test
    public void testRemove() {
        Set full = populatedSet(3);
        full.remove(one);
        assertFalse(full.contains(one));
        assertEquals(2, full.size());
    }

    *//**
     * keySet.size returns the number of elements
     *//*
    @Test
    public void testSize() {
        Set empty = ConcurrentHashMap.newKeySet();
        Set full = populatedSet(3);
        assertEquals(3, full.size());
        assertEquals(0, empty.size());
    }

    *//**
     * KeySet.toArray() returns an Object array containing all elements from the set
     *//*
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

    *//**
     * toArray(Integer array) returns an Integer array containing all elements from the set
     *//*
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

    *//**
     * A deserialized serialized set is equal
     *//*
    @Test
    public void testSerialization()   {
        int size = 20;
        Set x = populatedSet(size);
        Set y = serialClone(x);

        assertNotSame(x, y);
        assertEquals(x.size(), y.size());
        assertEquals(x, y);
        assertEquals(y, x);
    }

    // explicit function class to avoid type inference problems
    static class AddKeys implements BiFunction<Map.Entry<Long, Long>, Map.Entry<Long, Long>, Map.Entry<Long, Long>> {
        public Map.Entry<Long, Long> apply(Map.Entry<Long, Long> x, Map.Entry<Long, Long> y) {
            return new AbstractMap.SimpleEntry<Long, Long>
                    (Long.valueOf(x.getKey().longValue() + y.getKey().longValue()),
                            Long.valueOf(1L));
        }
    }

*/
}