/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.jsr166.map;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.jsr166.JSR166TestCase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * https://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

@SuppressWarnings({"rawtypes", "unchecked", "try"})
class ChronicleMapTest extends JSR166TestCase {

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
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     * clear removes all pairs
     */
    @Test
    @Timeout(5)
    void testClear() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.clear();
            assertEquals(0, map.size());
        }
    }

    /**
     * contains returns {@code true} for contained value
     */
    @Test
    @Timeout(5)
    void testContains() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * containsKey returns {@code true} for contained key
     */
    @Test
    @Timeout(5)
    void testContainsKey() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsKey(one));
            assertFalse(map.containsKey(zero));
        }
    }

    /**
     * containsValue returns {@code true} for held values
     */
    @Test
    @Timeout(5)
    void testContainsValue() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test
    @Timeout(5)
    void testGet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString());
            try (ChronicleMap<CharSequence, CharSequence> empty = newStrStrMap(8078)) {
                assertNull(map.get(notPresent));
            }
        }
    }

    /**
     * isEmpty is {@code true} of empty map and {@code false} for non-empty
     */
    @Test
    @Timeout(5)
    void testIsEmpty() throws IOException {
        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
            try (ChronicleMap<Integer, CharSequence> map = map5()) {
                if (!empty.isEmpty()) {
                    System.out.print("not empty " + empty);
                }
                assertTrue(empty.isEmpty());
                assertFalse(map.isEmpty());
            }
        }
    }

    /**
     * keySet returns a Set containing all the keys
     */
    @Test
    @Timeout(5)
    void testKeySet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            assertEquals(5, s.size());
            assertTrue(s.contains(one));
            assertTrue(s.contains(two));
            assertTrue(s.contains(three));
            assertTrue(s.contains(four));
            assertTrue(s.contains(five));
        }
    }

    /**
     * keySet.toArray returns contains all keys
     */
    @Test
    @Timeout(5)
    void testKeySetToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            Object[] ar = s.toArray();
            assertTrue(s.containsAll(Arrays.asList(ar)));
            assertEquals(5, ar.length);
            ar[0] = m10;
            assertFalse(s.containsAll(Arrays.asList(ar)));
        }
    }

    /**
     * Values.toArray contains all values
     */
    @Test
    @Timeout(5)
    void testValuesToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection<CharSequence> vs = map.values();
            ArrayList<CharSequence> s = new ArrayList<>(vs);
            assertEquals(5, s.size());
            assertTrue(s.stream().anyMatch("A"::contentEquals));
            assertTrue(s.stream().anyMatch("B"::contentEquals));
            assertTrue(s.stream().anyMatch("C"::contentEquals));
            assertTrue(s.stream().anyMatch("D"::contentEquals));
            assertTrue(s.stream().anyMatch("E"::contentEquals));
        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Test
    @Timeout(5)
    void testEntrySetToArray() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length);
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((Entry<?, ?>) (ar[i])).getKey()));
                assertTrue(map.containsValue(((Entry<?, ?>) (ar[i])).getValue()));
            }
        }
    }

    /**
     * values collection contains all values
     */
    @Test
    @Timeout(5)
    void testValues() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection s = map.values();
            assertEquals(5, s.size());
            assertTrue(s.contains("A"));
            assertTrue(s.contains("B"));
            assertTrue(s.contains("C"));
            assertTrue(s.contains("D"));
            assertTrue(s.contains("E"));
        }
    }

    /**
     * entrySet contains all pairs
     */
    @Test
    @Timeout(5)
    void testEntrySet() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set<Entry<Integer, CharSequence>> s = map.entrySet();
            assertEquals(5, s.size());
            for (Entry<Integer, CharSequence> e : s) {
                assertTrue(
                        (e.getKey().equals(one) && "A".contentEquals(e.getValue())) ||
                                (e.getKey().equals(two) && "B".contentEquals(e.getValue())) ||
                                (e.getKey().equals(three) && "C".contentEquals(e.getValue())) ||
                                (e.getKey().equals(four) && "D".contentEquals(e.getValue())) ||
                                (e.getKey().equals(five) && "E".contentEquals(e.getValue()))
                );
            }
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Test
    @Timeout(5)
    void testPutAll() throws IOException {

        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8076)) {
            try (ChronicleMap<Integer, CharSequence> map = map5()) {
                empty.putAll(map);
                assertEquals(5, empty.size());
                assertTrue(empty.containsKey(one));
                assertTrue(empty.containsKey(two));
                assertTrue(empty.containsKey(three));
                assertTrue(empty.containsKey(four));
                assertTrue(empty.containsKey(five));
            }
        }
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test
    @Timeout(5)
    void testPutIfAbsent() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.putIfAbsent(six, "Z");
            assertTrue(map.containsKey(six));
        }
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test
    @Timeout(5)
    void testPutIfAbsent2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.putIfAbsent(one, "Z").toString());
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test
    @Timeout(5)
    void testReplace() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNull(map.replace(six, "Z"));
            assertFalse(map.containsKey(six));
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test
    @Timeout(5)
    void testReplace2() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNotNull(map.replace(one, "Z"));
            assertEquals("Z", map.get(one).toString());
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test
    @Timeout(5)
    void testReplaceValue() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString());
            assertFalse(map.replace(one, "Z", "Z"));
            assertEquals("A", map.get(one).toString());
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test
    @Timeout(5)
    public void testReplaceValue2
    () throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(one).toString());
            assertTrue(map.replace(one, "A", "Z"));
            assertEquals("Z", map.get(one).toString());
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test
    @Timeout(5)
    void testRemove() throws
            IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(five);
            assertEquals(4, map.size());
            assertFalse(map.containsKey(five));
        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test
    @Timeout(5)
    void testRemove2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.remove(five, "E"));
            assertEquals(4, map.size());
            assertFalse(map.containsKey(five));
            assertFalse(map.remove(four, "A"));
            assertEquals(4, map.size());
            assertEquals("D", map.get(four).toString());
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    @Timeout(5)
    void testSize() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    @Timeout(10)
    void testSize2() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    @Timeout(5)
    void testSize3() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            try (ChronicleMap<Integer, CharSequence> empty = newShmIntString(8078)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * toString contains toString of elements
     */
    @Test
    @Timeout(5)
    void testToString() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            String s = map.toString();
            for (int i = 1; i <= 5; ++i) {
                assertTrue(s.contains(String.valueOf(i)));
            }
        }
    }

    /**
     * get(null) throws NPE
     */
    @Test
    @Timeout(5)
    void testGet_NullPointerException() throws IOException {

        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.get(null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * containsKey(null) throws NPE
     */
    @Test
    @Timeout(5)
    void testContainsKey_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.containsKey(null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Test
    @Timeout(5)
    void testPut1_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.put(null, "whatever");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Test
    @Timeout(5)
    public void testPut2_NullPointerException
    () throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.put(notPresent, null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test
    @Timeout(5)
    public void testPutIfAbsent1_NullPointerException
    () throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.putIfAbsent(null, "whatever");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test
    @Timeout(5)
    public void testReplace_NullPointerException
    () throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(null, "whatever");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test
    @Timeout(5)
    public void testReplaceValue_NullPointerException
    () throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(null, "A", "whatever");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test
    @Timeout(5)
    void testPutIfAbsent2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.putIfAbsent(notPresent, null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test
    @Timeout(5)
    void testReplace2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test
    @Timeout(5)
    void testReplaceValue2_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, null, "A");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test
    @Timeout(5)
    void testReplaceValue3_NullPointerException() throws IOException {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString(8076)) {
            c.replace(notPresent, "A", null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Test
    @Timeout(5)
    void testRemove1_NullPointerException() throws IOException {
        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test
    @Timeout(5)
    public void testRemove2_NullPointerException
    () throws IOException {
        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8086)) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            failExpectedException();
        } catch (NullPointerException | IllegalArgumentException success) {
            assertNotNull(success);
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Test
    @Timeout(5)
    void testRemove3() throws IOException {

        try (ChronicleMap<CharSequence, CharSequence> c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null));
        }
    }

    @Test
    void testPercentageComplete() {

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
                assertTrue(e.getMessage().contains("Attempt to allocate"));
            }
            long remainingAutoResizes = map.remainingAutoResizes();
            short percentageFreeSpace = map.percentageFreeSpace();
            assertEquals(0, (int) remainingAutoResizes);
            assertTrue(percentageFreeSpace < 6);
            ChronicleMap.SegmentStats[] segmentStats = map.segmentStats();
            assertEquals(3, segmentStats.length);
            for (ChronicleMap.SegmentStats ss : segmentStats) {
                assertEquals(50880, ss.usedBytes(), 600);
                assertEquals(52224, ss.sizeInBytes());
                assertEquals(3, ss.tiers());
            }
        }
    }
}
