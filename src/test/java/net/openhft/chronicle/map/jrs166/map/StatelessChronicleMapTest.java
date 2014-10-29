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

package net.openhft.chronicle.map.jrs166.map;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.hash.TcpReplicationConfig;
import net.openhft.chronicle.map.jrs166.JSR166TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static net.openhft.chronicle.hash.StatelessBuilder.remoteAddress;
import static org.junit.Assert.*;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */


public class StatelessChronicleMapTest extends JSR166TestCase {

    static class SingleCloseMap<K, V> implements ChronicleMap<K, V> {
        private final Closeable[] closeables;
        private final ChronicleMap<K, V> d;

        @Override
        public String toString() {
            return d.toString();
        }

        public SingleCloseMap(ChronicleMap<K, V> statelessMap, Closeable... closeables) {
            this.closeables = closeables;
            this.d = statelessMap;
        }

        @Override
        public File file() {
            return null;
        }

        @Override
        public void close() throws IOException {
            for (Closeable c : closeables) {
                c.close();
            }
        }

        @Override
        public long longSize() {
            return d.longSize();
        }

        @Override
        public int size() {
            return d.size();
        }

        @Override
        public boolean isEmpty() {
            return d.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return d.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return d.containsValue(value);
        }

        @Override
        public V get(Object key) {
            return d.get(key);
        }

        @Override
        public V put(K key, V value) {
            return d.put(key, value);
        }

        @Override
        public V remove(Object key) {
            return d.remove(key);
        }

        @Override
        public void putAll(java.util.Map<? extends K, ? extends V> m) {
            d.putAll(m);
        }

        @Override
        public void clear() {
            d.clear();
        }

        @NotNull
        @Override
        public Set<K> keySet() {
            return d.keySet();
        }

        @NotNull
        @Override
        public Collection<V> values() {
            return d.values();
        }

        @NotNull
        @Override
        public Set<Entry<K, V>> entrySet() {
            return d.entrySet();
        }

        @Override
        public V getUsing(K key, V usingValue) {
            return d.getUsing(key, usingValue);
        }

        @Override
        public V acquireUsing(K key, V usingValue) {
            return d.acquireUsing(key, usingValue);
        }

        @Override
        public V putIfAbsent(K key, V value) {
            return d.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            return d.remove(key, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            return d.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            return d.replace(key, value);
        }
    }

    static ChronicleMap<Integer, String> newShmIntString(int port) throws IOException {

        final ChronicleMap<Integer, String> serverMap = ChronicleMapBuilder.of(Integer.class, String.class)
                .replicators((byte) 1, TcpReplicationConfig.of(port)).create();

        final ChronicleMap<Integer, String> statelessMap = ChronicleMapBuilder.of(Integer
                .class, String.class)
                .stateless(remoteAddress(new InetSocketAddress("localhost", port))).create();

        return new SingleCloseMap(statelessMap, statelessMap, serverMap);

    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrMap(int port) throws
            IOException {

        final ChronicleMap<CharSequence, CharSequence> serverMap = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .replicators((byte) 2, TcpReplicationConfig.of(port)).create();


        final ChronicleMap<CharSequence, CharSequence> statelessMap = ChronicleMapBuilder.of(CharSequence
                .class, CharSequence.class)
                .stateless(remoteAddress(new InetSocketAddress("localhost", port))).create();

        return new SingleCloseMap(statelessMap, statelessMap, serverMap);
    }


    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     *
     * @param port
     */
    private ChronicleMap<Integer, String> map5(final int port) throws IOException {
        ChronicleMap<Integer, String> map = newShmIntString(port);
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
    public void testClear() throws IOException {
        try (ChronicleMap<Integer, String> map = map5(8076)) {

            map.clear();
            assertEquals(0, map.size());

        }
    }

    /**
     * contains returns true for contained value
     */
    @Test
    public void testContains() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }

    }

    /**
     * containsKey returns true for contained key
     */
    @Test
    public void testContainsKey() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertTrue(map.containsKey(one));
            assertFalse(map.containsKey(zero));
        }
    }

    /**
     * containsValue returns true for held values
     */
    @Test
    public void testContainsValue() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test
    public void testGet() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertEquals("A", (String) map.get(one));
            try (ChronicleMap empty = newStrStrMap(8078)) {
                assertNull(map.get(notPresent));
            }
        }
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    @Test
    public void testIsEmpty() throws IOException {
        try (ChronicleMap empty = newShmIntString(8078)) {
            try (ChronicleMap map = map5(8079)) {

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
    public void testKeySet() throws IOException {
        try (ChronicleMap map = map5(8076)) {
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
    public void testKeySetToArray() throws IOException {
        try (ChronicleMap map = map5(8076)) {
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
    public void testValuesToArray() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            Collection v = map.values();
            Object[] ar = v.toArray();
            ArrayList s = new ArrayList(Arrays.asList(ar));
            assertEquals(5, ar.length);
            assertTrue(s.contains("A"));
            assertTrue(s.contains("B"));
            assertTrue(s.contains("C"));
            assertTrue(s.contains("D"));
            assertTrue(s.contains("E"));

        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Test
    public void testEntrySetToArray() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length);
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((java.util.Map.Entry) (ar[i])).getKey()));
                assertTrue(map.containsValue(((java.util.Map.Entry) (ar[i])).getValue()));
            }

        }
    }

    /**
     * values collection contains all values
     */
    @Test
    public void testValues() throws IOException {
        try (ChronicleMap map = map5(8076)) {
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
    public void testEntrySet() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            Set s = map.entrySet();
            assertEquals(5, s.size());
            Iterator it = s.iterator();
            while (it.hasNext()) {
                java.util.Map.Entry e = (java.util.Map.Entry) it.next();
                assertTrue(
                        (e.getKey().equals(one) && e.getValue().equals("A")) ||
                                (e.getKey().equals(two) && e.getValue().equals("B")) ||
                                (e.getKey().equals(three) && e.getValue().equals("C")) ||
                                (e.getKey().equals(four) && e.getValue().equals("D")) ||
                                (e.getKey().equals(five) && e.getValue().equals("E"))
                );
            }

        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Test
    public void testPutAll() throws IOException {

        try (ChronicleMap empty = newShmIntString(8076)) {
            try (ChronicleMap map = map5(8076)) {
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
    public void testPutIfAbsent() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            map.putIfAbsent(six, "Z");
            assertTrue(map.containsKey(six));

        }
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test
    public void testPutIfAbsent2() throws    IOException {
        try (ChronicleMap map = map5(8076)) {
            assertEquals("A", map.putIfAbsent(one, "Z"));
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test
    public void testReplace() throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertNull(map.replace(six, "Z"));
            assertFalse(map.containsKey(six));

        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test
    public void testReplace2() throws
            IOException {
        try (ChronicleMap map = map5(8076)) {
            assertNotNull(map.replace(one, "Z"));
            assertEquals("Z", map.get(one));
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test
    public void testReplaceValue() throws
            IOException {
        try (ChronicleMap map = map5(8076)) {
            assertEquals("A", map.get(one));
            assertFalse(map.replace(one, "Z", "Z"));
            assertEquals("A", map.get(one));

        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test
    public void testReplaceValue2
    () throws IOException {
        try (ChronicleMap map = map5(8076)) {
            assertEquals("A", map.get(one));
            assertTrue(map.replace(one, "A", "Z"));
            assertEquals("Z", map.get(one));

        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test
    public void testRemove() throws
            IOException {
        try (ChronicleMap map = map5(8076)) {
            map.remove(five);
            assertEquals(4, map.size());
            assertFalse(map.containsKey(five));

        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test
    public void testRemove2
    () throws IOException {
   /*     try(   ChronicleMap map = map5(8076)) {
        map.remove(five, "E");
    assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
        map.remove(four, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(four));
   */
    }

    /**
     * size returns the correct values
     */
    @Test
    public void testSize() throws IOException {
        try (ChronicleMap map = map5(8076)) {

            try (ChronicleMap empty = newShmIntString(8078)) {
                assertEquals(0, empty.size()) ;
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test
    public void testSize2() throws IOException {
        try (ChronicleMap map = map5(8076)) {

            try (ChronicleMap empty = newShmIntString(8078)) {
                assertEquals(0, empty.size()) ;
                assertEquals(5, map.size());
            }
        }
    }
    /**
     * size returns the correct values
     */
    @Test
    public void testSize3() throws IOException {
        try (ChronicleMap map = map5(8076)) {

            try (ChronicleMap empty = newShmIntString(8078)) {
                assertEquals(0, empty.size()) ;
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * toString contains toString of elements
     */
    @Test
    public void testToString() throws IOException {
        try (ChronicleMap map = map5(8076)) {
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
    public void testGet_NullPointerException() throws IOException {

        try (ChronicleMap c = newShmIntString(8076)) {
            c.get(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }

    }

    /**
     * containsKey(null) throws NPE
     */
    @Test
    public void testContainsKey_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(8079)) {
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Test
    public void testPut1_NullPointerException   () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.put(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Test
    public void testPut2_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.put(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test
    public void testPutIfAbsent1_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }


    /**
     * replace(null, x) throws NPE
     */
    @Test
    public void testReplace_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.replace(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test
    public void testReplaceValue_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.replace(null, "A", "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }


    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test
    public void testPutIfAbsent2_NullPointerException () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.putIfAbsent(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test
    public void testReplace2_NullPointerException () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.replace(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test
    public void testReplaceValue2_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.replace(notPresent, null, "A");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test
    public void testReplaceValue3_NullPointerException    () throws IOException {
        try (ChronicleMap c = newShmIntString(8076)) {
            c.replace(notPresent, "A", null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Test
    public void testRemove1_NullPointerException() throws IOException {
        try (ChronicleMap c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test
    public void testRemove2_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newStrStrMap(8086)) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Test
    public void testRemove3() throws IOException {

        try (ChronicleMap c = newStrStrMap(8076)) {
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null));
        }
    }

    // classes for testing Comparable fallbacks
    static class BI implements Comparable<BI> {
        private final int value;

        BI(int value) {
            this.value = value;
        }

        public int compareTo(BI other) {
            return Integer.compare(value, other.value);
        }

        public boolean equals(Object x) {
            return (x instanceof BI) && ((BI) x).value == value;
        }

        public int hashCode() {
            return 42;
        }
    }

    static class CI extends BI {
        CI(int value) {
            super(value);
        }
    }

    static class DI extends BI {
        DI(int value) {
            super(value);
        }
    }

    static class BS implements Comparable<BS> {
        private final String value;

        BS(String value) {
            this.value = value;
        }

        public int compareTo(BS other) {
            return value.compareTo(other.value);
        }

        public boolean equals(Object x) {
            return (x instanceof BS) && value.equals(((BS) x).value);
        }

        public int hashCode() {
            return 42;
        }
    }

    static class LexicographicList<E extends Comparable<E>> extends ArrayList<E>
            implements Comparable<LexicographicList<E>> {
        private static final long serialVersionUID = 0;
        static long total;
        static long n;

        LexicographicList(Collection<E> c) {
            super(c);
        }

        LexicographicList(E e) {
            super(Collections.singleton(e));
        }

        public int compareTo(LexicographicList<E> other) {
            long start = System.currentTimeMillis();
            int common = Math.min(size(), other.size());
            int r = 0;
            for (int i = 0; i < common; i++) {
                if ((r = get(i).compareTo(other.get(i))) != 0)
                    break;
            }
            if (r == 0)
                r = Integer.compare(size(), other.size());
            total += System.currentTimeMillis() - start;
            n++;
            return r;
        }
    }


}

