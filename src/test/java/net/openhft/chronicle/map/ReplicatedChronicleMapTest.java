/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.map.jsr166.JSR166Case;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/*
 * Originally written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd. Then modified by the Open HFT team.
 */
@SuppressWarnings({"rawtypes", "unchecked", "PMD.UnnecessaryReturn", "PMD.UselessPureMethodCall"}) // legacy JSR166-style tests use early returns in catch blocks
public class ReplicatedChronicleMapTest extends JSR166Case {
    ChronicleMap<Integer, CharSequence> newShmIntString() {
        ChronicleMapBuilder<Integer, CharSequence> builder = ChronicleMap
                .of(Integer.class, CharSequence.class)
                .entries(1000)
                .averageValueSize(20);

        final ChronicleHashBuilderPrivateAPI<?, ?> privateAPI = Objects.requireNonNull(Jvm.getValue(builder,"privateAPI"));
        privateAPI.replication((byte) 1);
        return builder.create();
    }

    ChronicleMap<CharSequence, CharSequence> newShmStringString() {
        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMap
                .of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .averageKeySize(20)
                .averageValueSize(20);
        final ChronicleHashBuilderPrivateAPI<?, ?> privateAPI = Objects.requireNonNull(Jvm.getValue(builder,"privateAPI"));
        privateAPI.replication((byte) 1);
        return builder.create();
    }

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private ChronicleMap<Integer, CharSequence> map5() {
        ChronicleMap<Integer, CharSequence> map = newShmIntString();
        assertTrue(map.isEmpty(), "Newly created map should be empty");
        map.put(JSR166Case.one, "A");
        map.put(JSR166Case.two, "B");
        map.put(JSR166Case.three, "C");
        map.put(JSR166Case.four, "D");
        map.put(JSR166Case.five, "E");
        assertFalse(map.isEmpty(), "Map should not be empty after adding 5 entries");
        assertEquals(5, map.size(), "Map should contain exactly 5 entries");
        return map;
    }

    /**
     * clear removes all pairs
     */
    @Test
    public void testClear() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.clear();
            assertEquals(0, map.size(), "Map should be empty after clear operation");
        }
    }

    /**
     * Maps with same contents are equal
     */
    @Test
    public void testEquals() {
        try (ChronicleMap<Integer, CharSequence> map1 = map5();
             ChronicleMap<Integer, CharSequence> map2 = map5()) {
            assertEquals(map1, map2, "Maps with identical contents should be equal");
            assertEquals(map2, map1, "Map equality should be symmetric");
            map1.clear();
            assertNotEquals(map1, map2, "Empty map should not equal populated map");
            assertNotEquals(map2, map1, "Populated map should not equal empty map");
        }
    }

    /**
     * contains returns {@code true} for contained value
     */
    @Test
    public void testContains() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {

            assertTrue(map.containsValue("A"), "containsValue should return true for value 'A' present in map");
            assertFalse(map.containsValue("Z"), "containsValue should return false for value 'Z' not in map");
        }
    }

    /**
     * containsKey returns {@code true} for contained key
     */
    @Test
    public void testContainsKey() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsKey(JSR166Case.one), "Map should contain key 1 from initial entries");
            assertFalse(map.containsKey(JSR166Case.zero), "Map should not contain key 0 which was never added");
        }
    }

    /**
     * containsValue returns {@code true} for held values
     */
    @Test
    public void testContainsValue() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"), "containsValue should detect value 'A' among stored entries");
            assertFalse(map.containsValue("Z"), "containsValue should not detect value 'Z' never stored in map");
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test
    public void testGet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166Case.one).toString(), "Value for key 1 should be 'A'");
            ChronicleMap<Integer, CharSequence> empty = newShmIntString();
            assertNull(map.get(JSR166Case.notPresent), "Getting non-existent key should return null");
        }
    }

    /**
     * isEmpty is {@code true} of empty map and {@code false} for non-empty
     */
    @Test
    public void testIsEmpty() {
        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString();
             ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(empty.isEmpty(), "isEmpty should return true for map with no entries");
            assertFalse(map.isEmpty(), "isEmpty should return false for map with 5 entries");
        }
    }

    /**
     * keySet returns a Set containing all the keys
     */
    @Test
    public void testKeySet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            assertEquals(5, s.size(), "KeySet should contain all 5 keys from the map");
            assertTrue(s.contains(JSR166Case.one), "KeySet should contain key 1");
            assertTrue(s.contains(JSR166Case.two), "KeySet should contain key 2");
            assertTrue(s.contains(JSR166Case.three), "KeySet should contain key 3");
            assertTrue(s.contains(JSR166Case.four), "KeySet should contain key 4");
            assertTrue(s.contains(JSR166Case.five), "KeySet should contain key 5");
        }
    }

    /**
     * keySet.toArray returns contains all keys
     */
    @Test
    public void testKeySetToArray() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            Object[] ar = s.toArray();
            assertTrue(s.containsAll(Arrays.asList(ar)), "KeySet should contain all elements from its toArray() result");
            assertEquals(5, ar.length, "Array should contain all 5 keys");
            ar[0] = JSR166Case.m10;
            assertFalse(s.containsAll(Arrays.asList(ar)), "KeySet should not contain modified array with invalid key");
        }
    }

    /**
     * Values.toArray contains all values
     */
    @Test
    public void testValuesToArray() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection v = map.values();
            ArrayList<CharSequence> s = new ArrayList<>(map.values());
            assertEquals(5, s.size(), "values collection converted to array should preserve all 5 entries");
            assertTrue(s.stream().anyMatch("A"::contentEquals), "array from values should include entry 'A'");
            assertTrue(s.stream().anyMatch("B"::contentEquals), "array from values should include entry 'B'");
            assertTrue(s.stream().anyMatch("C"::contentEquals), "array from values should include entry 'C'");
            assertTrue(s.stream().anyMatch("D"::contentEquals), "array from values should include entry 'D'");
            assertTrue(s.stream().anyMatch("E"::contentEquals), "array from values should include entry 'E'");
        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Test
    public void testEntrySetToArray() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length, "Array should contain all 5 entries");
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((Map.Entry<?, ?>) (ar[i])).getKey()), "Map should contain key from entry " + i);
                assertTrue(map.containsValue(((Map.Entry<?, ?>) (ar[i])).getValue()), "Map should contain value from entry " + i);
            }
        }
    }

    /**
     * values collection contains all values
     */
    @Test
    public void testValues() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Collection s = map.values();
            assertEquals(5, s.size(), "values collection should reflect map size of 5 entries");
            assertTrue(s.contains("A"), "values collection should include stored value 'A'");
            assertTrue(s.contains("B"), "values collection should include stored value 'B'");
            assertTrue(s.contains("C"), "values collection should include stored value 'C'");
            assertTrue(s.contains("D"), "values collection should include stored value 'D'");
            assertTrue(s.contains("E"), "values collection should include stored value 'E'");
        }
    }

    /**
     * entrySet contains all pairs
     */
    @Test
    public void testEntrySet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            assertEquals(5, s.size(), "EntrySet should contain all 5 key-value pairs from the map");
            for (Object o : s) {
                Map.Entry<Integer, CharSequence> e = (Map.Entry<Integer, CharSequence>) o;
                assertTrue(
                        (e.getKey().equals(JSR166Case.one) &&
                                "A".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166Case.two) &&
                                        "B".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166Case.three) &&
                                        "C".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166Case.four) &&
                                        "D".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166Case.five) &&
                                        "E".contentEquals(e.getValue()))
                , "Each entry should match one of the expected key-value pairs (1->A, 2->B, 3->C, 4->D, 5->E)");
            }
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Test
    public void testPutAll() {
        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString()) {
            ChronicleMap<Integer, CharSequence> map = map5();
            empty.putAll(map);
            assertEquals(5, empty.size(), "Map should contain all 5 entries after putAll operation");
            assertTrue(empty.containsKey(JSR166Case.one), "Map should contain key 1 after putAll");
            assertTrue(empty.containsKey(JSR166Case.two), "Map should contain key 2 after putAll");
            assertTrue(empty.containsKey(JSR166Case.three), "Map should contain key 3 after putAll");
            assertTrue(empty.containsKey(JSR166Case.four), "Map should contain key 4 after putAll");
            assertTrue(empty.containsKey(JSR166Case.five), "Map should contain key 5 after putAll");
        }
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test
    public void testPutIfAbsent() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.putIfAbsent(JSR166Case.six, "Z");
            assertTrue(map.containsKey(JSR166Case.six), "Map should contain newly added key 6 after putIfAbsent");
        }
    }

    /**
     * putIfAbsent does not add the §pair if the key is already present
     */
    @Test
    public void testPutIfAbsent2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.putIfAbsent(JSR166Case.one, "Z").toString(), "putIfAbsent should return existing value 'A' when key is already present");
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test
    public void testReplace() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNull(map.replace(JSR166Case.six, "Z"), "replace should return null when key is not present");
            assertFalse(map.containsKey(JSR166Case.six), "Map should not contain key 6 after failed replace");
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test
    public void testReplace2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNotNull(map.replace(JSR166Case.one, "Z"), "replace should return old value when key is present");
            assertEquals("Z", map.get(JSR166Case.one).toString(), "Value for key 1 should be updated to 'Z' after replace");
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test
    public void testReplaceValue() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166Case.one).toString(), "key 1 should initially map to value 'A' before replace attempt");
            assertFalse(map.replace(JSR166Case.one, "Z", "Z"), "replace should return false when old value doesn't match");
            assertEquals("A", map.get(JSR166Case.one).toString(), "Value should remain 'A' after failed conditional replace");
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test
    public void testReplaceValue2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166Case.one).toString(), "key 1 should map to 'A' before successful replace operation");
            assertTrue(map.replace(JSR166Case.one, "A", "Z"), "replace should return true when old value matches");
            assertEquals("Z", map.get(JSR166Case.one).toString(), "Value should be updated to 'Z' after successful conditional replace");
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test
    public void testRemove() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(JSR166Case.five);
            assertEquals(4, map.size(), "Map should have 4 entries after removing one");
            assertFalse(map.containsKey(JSR166Case.five), "Map should not contain removed key 5");
        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test
    public void testRemove2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(JSR166Case.five, "E");
            assertEquals(4, map.size(), "Map should have 4 entries after removing matching key-value pair");
            assertFalse(map.containsKey(JSR166Case.five), "Map should not contain key 5 after successful conditional remove");
            map.remove(JSR166Case.four, "A");
            assertEquals(4, map.size(), "Map size should remain 4 when remove value doesn't match");
            assertTrue(map.containsKey(JSR166Case.four), "Map should still contain key 4 after failed conditional remove");
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    public void testSize() {
        try (ChronicleMap<Integer, CharSequence> map = map5();
             ChronicleMap<Integer, CharSequence> empty = newShmIntString()) {
            assertEquals(0, empty.size(), "Empty map should have size 0");
            assertEquals(5, map.size(), "Map with 5 entries should have size 5");
        }
    }

    /**
     * toString contains toString of elements
     */
    @Test
    public void testToString() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            String s = map.toString();
            for (int i = 1; i <= 5; ++i) {
                assertTrue(s.contains(String.valueOf(i)), "toString output should contain key " + i);
            }
        }
    }

    /**
     * get(null) throws NPE
     */
    @Test
    public void testGet_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.get(null);
            fail("get(null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * containsKey(null) throws NPE
     */
    @Test
    public void testContainsKey_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {

            c.containsKey(null);
            fail("containsKey(null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Test
    public void testPut1_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.put(null, "whatever");
            fail("put(null, value) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Test
    public void testPut2_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.put(JSR166Case.notPresent, null);
            fail("put(key, null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test
    public void testPutIfAbsent1_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.putIfAbsent(null, "whatever");
            fail("putIfAbsent(null, value) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test
    public void testReplace_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.replace(null, "whatever");
            fail("replace(null, value) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test
    public void testReplaceValue_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.replace(null, "A", "whatever");
            fail("replace(null, oldValue, newValue) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test
    public void testPutIfAbsent2_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.putIfAbsent(JSR166Case.notPresent, null);
            fail("putIfAbsent(key, null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test
    public void testReplace2_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.replace(JSR166Case.notPresent, null);
            fail("replace(key, null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test
    public void testReplaceValue2_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.replace(JSR166Case.notPresent, null, "A");
            fail("replace(key, null, newValue) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test
    public void testReplaceValue3_NullPointerException() {
        try (ChronicleMap<Integer, CharSequence> c = newShmIntString()) {
            c.replace(JSR166Case.notPresent, "A", null);
            fail("replace(key, oldValue, null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Test
    public void testRemove1_NullPointerException() {
        try (ChronicleMap<CharSequence, CharSequence> c = newShmStringString()) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            fail("remove(null) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test
    public void testRemove2_NullPointerException() {
        try (ChronicleMap<CharSequence, CharSequence> c = newShmStringString()) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            fail("remove(null, value) should throw NullPointerException or IllegalArgumentException");
        } catch (NullPointerException | IllegalArgumentException success) {
            // expected
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Test
    public void testRemove3() {
        try (ChronicleMap<CharSequence, CharSequence> c = newShmStringString()) {
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null), "remove should return false when attempting to remove with null value");
        }
    }
}
