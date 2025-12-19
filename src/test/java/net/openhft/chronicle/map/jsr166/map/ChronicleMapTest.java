/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.jsr166.map;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.jsr166.JSR166Case;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

@SuppressWarnings({"rawtypes", "unchecked", "try", "PMD.UselessPureMethodCall"})
public class ChronicleMapTest extends JSR166Case {

    static ChronicleMap<Integer, CharSequence> newShmIntString(int size) {
        return ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .averageValueSize(1)
                .entries(size).create();
    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrMap(int size) {
        return ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .averageKeySize(20).averageValueSize(20)
                .entries(size).create();
    }

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private static ChronicleMap<Integer, CharSequence> map5() throws IOException {
        ChronicleMap<Integer, CharSequence> map = newShmIntString(10);
        assertTrue(map.isEmpty(), "newly created map should be empty");
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty(), "map with five entries should not be empty");
        assertEquals(5, map.size(), "map should contain exactly five entries");
        return map;
    }

    /**
     * clear removes all pairs
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testClear() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.clear();
            assertEquals(0, map.size(), "cleared map should have size zero");
        }
    }

    /**
     * contains returns {@code true} for contained value
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testContains() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"), "map should contain value from populated map");
            assertFalse(map.containsValue("Z"), "map should not contain value that was never added");
        }
    }

    /**
     * containsKey returns {@code true} for contained key
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testContainsKey() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsKey(one), "map should contain existing key one");
            assertFalse(map.containsKey(zero), "map should not contain key zero that was never added");
        }
    }

    /**
     * containsValue returns {@code true} for held values
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testContainsValue() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"), "map should contain existing value A");
            assertFalse(map.containsValue("Z"), "map should not contain non-existent value Z");
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testGet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString(), "key one should map to value A");
            try (ChronicleMap<CharSequence, CharSequence> empty = newStrStrMap(8078)) {
                assertNull(map.get(notPresent), "map should return null for key that was never added");
            }
        }
    }

    /**
     * isEmpty is {@code true} of empty map and {@code false} for non-empty
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testIsEmpty() throws IOException {
        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
            try (ChronicleMap<Integer, CharSequence> map = map5()) {
                if (!empty.isEmpty()) {
                    System.out.print("not empty " + empty);
                }
                assertTrue(empty.isEmpty(), "newly created map should be empty");
                assertFalse(map.isEmpty(), "map with five entries should not be empty");
            }
        }
    }

    /**
     * keySet returns a Set containing all the keys
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testKeySet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            assertEquals(5, s.size(), "key set should contain five keys");
            assertTrue(s.contains(one), "key set should contain key one");
            assertTrue(s.contains(two), "key set should contain key two");
            assertTrue(s.contains(three), "key set should contain key three");
            assertTrue(s.contains(four), "key set should contain key four");
            assertTrue(s.contains(five), "key set should contain key five");
        }
    }

    /**
     * keySet.toArray returns contains all keys
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testKeySetToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            Object[] ar = s.toArray();
            assertTrue(s.containsAll(Arrays.asList(ar)), "key set should contain all elements from its own toArray");
            assertEquals(5, ar.length, "array should contain five keys");
            ar[0] = m10;
            assertFalse(s.containsAll(Arrays.asList(ar)), "key set should not contain all elements after array modification");
        }
    }

    /**
     * Values.toArray contains all values
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testValuesToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection<CharSequence> vs = map.values();
            ArrayList<CharSequence> s = new ArrayList<>(vs);
            assertEquals(5, s.size(), "values collection should contain five values");
            assertTrue(s.stream().anyMatch("A"::contentEquals), "values should contain A");
            assertTrue(s.stream().anyMatch("B"::contentEquals), "values should contain B");
            assertTrue(s.stream().anyMatch("C"::contentEquals), "values should contain C");
            assertTrue(s.stream().anyMatch("D"::contentEquals), "values should contain D");
            assertTrue(s.stream().anyMatch("E"::contentEquals), "values should contain E");
        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testEntrySetToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length, "array should contain five entries");
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((Entry<?, ?>) (ar[i])).getKey()), "map should contain key from entry at index " + i);
                assertTrue(map.containsValue(((Entry<?, ?>) (ar[i])).getValue()), "map should contain value from entry at index " + i);
            }
        }
    }

    /**
     * values collection contains all values
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testValues() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection s = map.values();
            assertEquals(5, s.size(), "values collection should contain five values");
            assertTrue(s.contains("A"), "values should contain A");
            assertTrue(s.contains("B"), "values should contain B");
            assertTrue(s.contains("C"), "values should contain C");
            assertTrue(s.contains("D"), "values should contain D");
            assertTrue(s.contains("E"), "values should contain E");
        }
    }

    /**
     * entrySet contains all pairs
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testEntrySet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set<Entry<Integer, CharSequence>> s = map.entrySet();
            assertEquals(5, s.size(), "entry set should contain five entries");
            for (Entry<Integer, CharSequence> e : s) {
                assertTrue(
                        (e.getKey().equals(one) && "A".contentEquals(e.getValue())) ||
                                (e.getKey().equals(two) && "B".contentEquals(e.getValue())) ||
                                (e.getKey().equals(three) && "C".contentEquals(e.getValue())) ||
                                (e.getKey().equals(four) && "D".contentEquals(e.getValue())) ||
                                (e.getKey().equals(five) && "E".contentEquals(e.getValue()))
                , "each entry should match one of the five expected key-value pairs");
            }
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPutAll() throws IOException {

        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8076)) {
            try (ChronicleMap<Integer, CharSequence> map = map5()) {
                empty.putAll(map);
                assertEquals(5, empty.size(), "map should contain all five entries after putAll");
                assertTrue(empty.containsKey(one), "map should contain key one after putAll");
                assertTrue(empty.containsKey(two), "map should contain key two after putAll");
                assertTrue(empty.containsKey(three), "map should contain key three after putAll");
                assertTrue(empty.containsKey(four), "map should contain key four after putAll");
                assertTrue(empty.containsKey(five), "map should contain key five after putAll");
            }
        }
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPutIfAbsent() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.putIfAbsent(six, "Z");
            assertTrue(map.containsKey(six), "map should contain key six after putIfAbsent with new key");
        }
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPutIfAbsent2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.putIfAbsent(one, "Z").toString(), "putIfAbsent should return existing value A when key is already present");
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplace() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNull(map.replace(six, "Z"), "replace should return null when key is not present");
            assertFalse(map.containsKey(six), "map should not contain key six after failed replace");
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplace2() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNotNull(map.replace(one, "Z"), "replace should return previous value when key exists");
            assertEquals("Z", map.get(one).toString(), "key one should map to new value Z after replace");
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplaceValue() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString(), "key one should initially map to value A");
            assertFalse(map.replace(one, "Z", "Z"), "replace should fail when old value does not match");
            assertEquals("A", map.get(one).toString(), "key one should still map to value A after failed replace");
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplaceValue2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString(), "key one should initially map to value A");
            assertTrue(map.replace(one, "A", "Z"), "replace should succeed when old value matches");
            assertEquals("Z", map.get(one).toString(), "key one should map to value Z after successful replace");
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testRemove() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(five);
            assertEquals(4, map.size(), "map should contain four entries after removing one");
            assertFalse(map.containsKey(five), "map should not contain removed key five");
        }
    }

    /**
     * size returns the correct values
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testSize() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size(), "empty map should have size zero");
                assertEquals(5, map.size(), "populated map should have size five");
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testSize2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size(), "empty map should have size zero");
                assertEquals(5, map.size(), "populated map should have size five");
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testSize3() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size(), "empty map should have size zero");
                assertEquals(5, map.size(), "populated map should have size five");
            }
        }
    }

    /**
     * toString contains toString of elements
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testToString() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            String s = map.toString();
            for (int i = 1; i <= 5; ++i) {
                assertTrue(s.contains(String.valueOf(i)), "toString should contain key " + i);
            }
        }
    }

    /**
     * get(null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testGet_NullPointerException() throws IOException {

        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.get(null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "get(null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * containsKey(null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testContainsKey_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.containsKey(null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "containsKey(null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPut1_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.put(null, "whatever");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "put(null, value) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPut2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.put(notPresent, null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "put(key, null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPutIfAbsent1_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.putIfAbsent(null, "whatever");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "putIfAbsent(null, value) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * replace(null, x) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplace_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(null, "whatever");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "replace(null, value) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplaceValue_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(null, "A", "whatever");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "replace(null, oldValue, newValue) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testPutIfAbsent2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.putIfAbsent(notPresent, null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "putIfAbsent(key, null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplace2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "replace(key, null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplaceValue2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, null, "A");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "replace(key, null, newValue) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testReplaceValue3_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, "A", null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "replace(key, oldValue, null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testRemove1_NullPointerException() throws IOException {
        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "remove(null) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testRemove2_NullPointerException() throws IOException {
        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8086)) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            assertThrows();
        } catch (NullPointerException | IllegalArgumentException success) {
            Assertions.assertNotNull(success, "remove(null, value) should throw NullPointerException or IllegalArgumentException");
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testRemove3() throws IOException {

        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null), "remove should return false when value is null");
        }
    }

    @Test
    public void testPercentageComplete() {

        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1600)
                .actualSegments(3)
                .maxBloatFactor(2)
                .create()) {

            try {
                for (int i = 0; ; i++) {
                    map.put(i, 0);
                }
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempt to allocate"), "exception message should indicate allocation failure");
            }
            long remainingAutoResizes = map.remainingAutoResizes();
            short percentageFreeSpace = map.percentageFreeSpace();
            Assertions.assertEquals(0, (int) remainingAutoResizes, "remaining auto resizes should be zero when map is full");
            Assertions.assertTrue(percentageFreeSpace < 6, "percentage free space should be less than 6 percent when map is full");
            ChronicleMap.SegmentStats[] segmentStats = map.segmentStats();
            assertEquals(3, segmentStats.length, "segment stats array should contain three segments");
            for (ChronicleMap.SegmentStats ss : segmentStats) {
                assertEquals(50880, ss.usedBytes(), 600, "used bytes should be approximately 50880 with delta 600");
                assertEquals(52224, ss.sizeInBytes(), "size in bytes should be 52224");
                assertEquals(3, ss.tiers(), "tiers should be 3");
            }
        }
    }
}
