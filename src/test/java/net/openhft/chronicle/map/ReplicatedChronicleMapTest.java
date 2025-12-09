/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.map.jsr166.JSR166TestCase;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/*
 * Originally written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd. Then modified by the Open HFT team.
 */
@SuppressWarnings({"rawtypes", "unchecked", "PMD.UnnecessaryReturn", "PMD.UselessPureMethodCall"}) // legacy JSR166-style tests use early returns in catch blocks
public class ReplicatedChronicleMapTest extends JSR166TestCase {
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
        assertTrue(map.isEmpty());
        map.put(JSR166TestCase.one, "A");
        map.put(JSR166TestCase.two, "B");
        map.put(JSR166TestCase.three, "C");
        map.put(JSR166TestCase.four, "D");
        map.put(JSR166TestCase.five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     * clear removes all pairs
     */
    @Test
    public void testClear() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.clear();
            assertEquals(0, map.size());
        }
    }

    /**
     * Maps with same contents are equal
     */
    @Test
    public void testEquals() {
        try (ChronicleMap<Integer, CharSequence> map1 = map5();
             ChronicleMap<Integer, CharSequence> map2 = map5()) {
            assertEquals(map1, map2);
            assertEquals(map2, map1);
            map1.clear();
            assertNotEquals(map1, map2);
            assertNotEquals(map2, map1);
        }
    }

    /**
     * contains returns {@code true} for contained value
     */
    @Test
    public void testContains() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {

            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * containsKey returns {@code true} for contained key
     */
    @Test
    public void testContainsKey() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsKey(JSR166TestCase.one));
            assertFalse(map.containsKey(JSR166TestCase.zero));
        }
    }

    /**
     * containsValue returns {@code true} for held values
     */
    @Test
    public void testContainsValue() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test
    public void testGet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166TestCase.one).toString());
            ChronicleMap<Integer, CharSequence> empty = newShmIntString();
            assertNull(map.get(JSR166TestCase.notPresent));
        }
    }

    /**
     * isEmpty is {@code true} of empty map and {@code false} for non-empty
     */
    @Test
    public void testIsEmpty() {
        try (ChronicleMap<Integer, CharSequence> empty = newShmIntString();
             ChronicleMap<Integer, CharSequence> map = map5()) {
            assertTrue(empty.isEmpty());
            assertFalse(map.isEmpty());
        }
    }

    /**
     * keySet returns a Set containing all the keys
     */
    @Test
    public void testKeySet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.keySet();
            assertEquals(5, s.size());
            assertTrue(s.contains(JSR166TestCase.one));
            assertTrue(s.contains(JSR166TestCase.two));
            assertTrue(s.contains(JSR166TestCase.three));
            assertTrue(s.contains(JSR166TestCase.four));
            assertTrue(s.contains(JSR166TestCase.five));
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
            assertTrue(s.containsAll(Arrays.asList(ar)));
            assertEquals(5, ar.length);
            ar[0] = JSR166TestCase.m10;
            assertFalse(s.containsAll(Arrays.asList(ar)));
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
    public void testEntrySetToArray() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length);
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((Map.Entry<?, ?>) (ar[i])).getKey()));
                assertTrue(map.containsValue(((Map.Entry<?, ?>) (ar[i])).getValue()));
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
    public void testEntrySet() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            Set s = map.entrySet();
            assertEquals(5, s.size());
            for (Object o : s) {
                Map.Entry<Integer, CharSequence> e = (Map.Entry<Integer, CharSequence>) o;
                assertTrue(
                        (e.getKey().equals(JSR166TestCase.one) &&
                                "A".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166TestCase.two) &&
                                        "B".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166TestCase.three) &&
                                        "C".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166TestCase.four) &&
                                        "D".contentEquals(e.getValue())) ||
                                (e.getKey().equals(JSR166TestCase.five) &&
                                        "E".contentEquals(e.getValue()))
                );
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
            assertEquals(5, empty.size());
            assertTrue(empty.containsKey(JSR166TestCase.one));
            assertTrue(empty.containsKey(JSR166TestCase.two));
            assertTrue(empty.containsKey(JSR166TestCase.three));
            assertTrue(empty.containsKey(JSR166TestCase.four));
            assertTrue(empty.containsKey(JSR166TestCase.five));
        }
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test
    public void testPutIfAbsent() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.putIfAbsent(JSR166TestCase.six, "Z");
            assertTrue(map.containsKey(JSR166TestCase.six));
        }
    }

    /**
     * putIfAbsent does not add the §pair if the key is already present
     */
    @Test
    public void testPutIfAbsent2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.putIfAbsent(JSR166TestCase.one, "Z").toString());
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test
    public void testReplace() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNull(map.replace(JSR166TestCase.six, "Z"));
            assertFalse(map.containsKey(JSR166TestCase.six));
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test
    public void testReplace2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertNotNull(map.replace(JSR166TestCase.one, "Z"));
            assertEquals("Z", map.get(JSR166TestCase.one).toString());
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test
    public void testReplaceValue() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166TestCase.one).toString());
            assertFalse(map.replace(JSR166TestCase.one, "Z", "Z"));
            assertEquals("A", map.get(JSR166TestCase.one).toString());
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test
    public void testReplaceValue2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            assertEquals("A", map.get(JSR166TestCase.one).toString());
            assertTrue(map.replace(JSR166TestCase.one, "A", "Z"));
            assertEquals("Z", map.get(JSR166TestCase.one).toString());
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test
    public void testRemove() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(JSR166TestCase.five);
            assertEquals(4, map.size());
            assertFalse(map.containsKey(JSR166TestCase.five));
        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test
    public void testRemove2() {
        try (ChronicleMap<Integer, CharSequence> map = map5()) {
            map.remove(JSR166TestCase.five, "E");
            assertEquals(4, map.size());
            assertFalse(map.containsKey(JSR166TestCase.five));
            map.remove(JSR166TestCase.four, "A");
            assertEquals(4, map.size());
            assertTrue(map.containsKey(JSR166TestCase.four));
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    public void testSize() {
        try (ChronicleMap<Integer, CharSequence> map = map5();
             ChronicleMap<Integer, CharSequence> empty = newShmIntString()) {
            assertEquals(0, empty.size());
            assertEquals(5, map.size());
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
                assertTrue(s.contains(String.valueOf(i)));
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
            assertThrows();
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
            assertThrows();
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
            assertThrows();
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
            c.put(JSR166TestCase.notPresent, null);
            assertThrows();
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
            assertThrows();
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
            assertThrows();
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
            assertThrows();
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
            c.putIfAbsent(JSR166TestCase.notPresent, null);
            assertThrows();
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
            c.replace(JSR166TestCase.notPresent, null);
            assertThrows();
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
            c.replace(JSR166TestCase.notPresent, null, "A");
            assertThrows();
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
            c.replace(JSR166TestCase.notPresent, "A", null);
            assertThrows();
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
            assertThrows();
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
            assertThrows();
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
            assertFalse(c.remove("sadsdf", null));
        }
    }
}
