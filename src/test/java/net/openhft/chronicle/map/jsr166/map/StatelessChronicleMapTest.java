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

package net.openhft.chronicle.map.jsr166.map;

import net.openhft.chronicle.hash.function.Consumer;
import net.openhft.chronicle.hash.function.Function;
import net.openhft.chronicle.hash.function.Predicate;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.jsr166.JSR166TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static net.openhft.chronicle.map.StatelessClientTest.localClient;
import static org.junit.Assert.*;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

public class StatelessChronicleMapTest extends JSR166TestCase {

    public static final int SIZE = 1024;

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
        public void close() {
            for (Closeable c : closeables) {
                try {
                    c.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    // eat the exception
                }
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
        public MapKeyContext<K, V> context(K key) {
            return d.context(key);
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
        public V acquireUsing(@NotNull K key, V usingValue) {
            return d.acquireUsing(key, usingValue);
        }

        @NotNull
        @Override
        public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <R> R getMapped(K key, @NotNull Function<? super V, R> function) {
            return d.getMapped(key, function);
        }

        @Override
        public  V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
            return d.putMapped(key, unaryOperator);
        }

        @Override
        public void getAll(File toFile) throws IOException {
            d.getAll(toFile);
        }

        @Override
        public void putAll(File fromFile) throws IOException {
            d.putAll(fromFile);
        }

        @Override
        public V newValueInstance() {
            throw new UnsupportedOperationException();
        }

        @Override
        public K newKeyInstance() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Class<K> keyClass() {
            return d.keyClass();
        }

        @Override
        public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
            return d.forEachEntryWhile(predicate);
        }

        @Override
        public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {
            d.forEachEntry(action);
        }

        @Override
        public Class<V> valueClass() {
            return d.valueClass();
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

        final ChronicleMap<Integer, String> serverMap =
                ChronicleMapBuilder.of(Integer.class, String.class)
                        .averageValueSize(1)
                        .entries(SIZE)
                        .replication((byte) 1, TcpTransportAndNetworkConfig.of(port))
                        .create();

        final ChronicleMap<Integer, String> statelessMap = localClient(port);

        return new SingleCloseMap(statelessMap, statelessMap, serverMap);
    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrMap(int port) throws
            IOException {

        final ChronicleMap<CharSequence, CharSequence> serverMap = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port)).create();

        final ChronicleMap<CharSequence, CharSequence> statelessMap = localClient(port);

        return new SingleCloseMap<>(statelessMap, statelessMap, serverMap);
    }

    static ChronicleMap<byte[], byte[]> newByteArrayMap(int port) throws IOException {

        final ChronicleMap<byte[], byte[]> serverMap = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .putReturnsNull(true)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create();

        final ChronicleMap<byte[], byte[]> statelessMap = ChronicleMapBuilder
                .of(byte[].class, byte[].class, new InetSocketAddress("localhost", port))
                .putReturnsNull(true)
                .create();

        return new SingleCloseMap<>(statelessMap, statelessMap, serverMap);
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
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

    static int s_port = 11050;

    @Test(timeout = 30000)
    @Ignore
    public void testByteArrayPerf() throws IOException {
        ByteBuffer key = ByteBuffer.allocate(8);
        ByteBuffer value = ByteBuffer.allocate(50);
        int runs = 100000;
        try (ChronicleMap<byte[], byte[]> map = newByteArrayMap(s_port++)) {
            for (int t = 0; t < 5; t++) {
                {
                    long start = System.nanoTime();
                    for (int i = 0; i < runs; i++) {
                        key.putLong(0, i);
                        value.putLong(0, i);
                        map.put(key.array(), value.array());
                    }
                    long time = System.nanoTime() - start;
                    System.out.printf("%d: Took %.1f us on average to put %n", t, time / 1e3 / runs);
                }
                {
                    long start = System.nanoTime();
                    for (int i = 0; i < runs; i++) {
                        key.putLong(0, i);
                        byte[] b = map.get(key.array());
                    }
                    long time = System.nanoTime() - start;
                    System.out.printf("%d: Took %.1f us on average to get %n", t, time / 1e3 / runs);
                }
            }
        }
    }

    /**
     * clear removes all pairs
     */
    @Test(timeout = 10000)
    public void testClear() throws IOException {
        try (ChronicleMap<Integer, String> map = map5(s_port++)) {
            map.clear();
            assertEquals(0, map.size());
        }
    }

    /**
     * contains returns true for contained value
     */
    @Test(timeout = 10000)
    public void testContains() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * containsKey returns true for contained key
     */
    @Test(timeout = 10000)
    public void testContainsKey() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertTrue(map.containsKey(one));
            assertFalse(map.containsKey(zero));
        }
    }

    /**
     * containsValue returns true for held values
     */
    @Test(timeout = 10000)
    public void testContainsValue() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test(timeout = 10000)
    public void testGet() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertEquals("A", (String) map.get(one));
            try (ChronicleMap empty = newStrStrMap(8078)) {
                assertNull(map.get(notPresent));
            }
        }
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    @Test(timeout = 10000)
    public void testIsEmpty() throws IOException {
        try (ChronicleMap empty = newShmIntString(s_port++)) {
            try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 10000)
    public void testKeySet() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 10000)
    public void testKeySetToArray() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 10000)
    public void testValuesToArray() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 5000)
    public void testEntrySetToArray() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 5000)
    public void testValues() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 10000)
    public void testEntrySet() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
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
    @Test(timeout = 5000)
    public void testPutAll() throws IOException {
        int port = s_port++;
        try (ChronicleMap empty = newShmIntString(port)) {
            try (ChronicleMap map = map5(port)) {
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
    @Test(timeout = 5000)
    public void testPutIfAbsent() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            map.putIfAbsent(six, "Z");
            assertTrue(map.containsKey(six));
        }
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test(timeout = 5000)
    public void testPutIfAbsent2() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertEquals("A", map.putIfAbsent(one, "Z"));
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test(timeout = 5000)
    public void testReplace() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertNull(map.replace(six, "Z"));
            assertFalse(map.containsKey(six));
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test(timeout = 5000)
    public void testReplace2() throws
            IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertNotNull(map.replace(one, "Z"));
            assertEquals("Z", map.get(one));
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test(timeout = 5000)
    public void testReplaceValue() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertEquals("A", map.get(one));
            assertFalse(map.replace(one, "Z", "Z"));
            assertEquals("A", map.get(one));
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test(timeout = 5000)
    public void testReplaceValue2() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            assertEquals("A", map.get(one));
            assertTrue(map.replace(one, "A", "Z"));
            assertEquals("Z", map.get(one));
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test(timeout = 5000)
    public void testRemove() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            map.remove(five);
            assertEquals(4, map.size());
            assertFalse(map.containsKey(five));
        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test(timeout = 5000)
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
    @Test(timeout = 5000)
    public void testSize() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            try (ChronicleMap empty = newShmIntString(s_port++)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test(timeout = 15000)
    public void testSize2() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            try (ChronicleMap empty = newShmIntString(s_port++)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test(timeout = 5000)
    public void testSize3() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            try (ChronicleMap empty = newShmIntString(s_port++)) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * toString contains toString of elements
     */
    @Test(timeout = 5000)
    public void testToString() throws IOException {
        try (ChronicleMap map = map5(s_port++)) {
            String s = map.toString();
            for (int i = 1; i <= 5; ++i) {
                assertTrue(s.contains(String.valueOf(i)));
            }
        }
    }

    /**
     * get(null) throws NPE
     */
    @Test(timeout = 5000)
    public void testGet_NullPointerException() throws IOException {

        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.get(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * containsKey(null) throws NPE
     */
    @Test(timeout = 5000)
    public void testContainsKey_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Test(timeout = 5000)
    public void testPut1_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.put(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Test(timeout = 5000)
    public void testPut2_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.put(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test(timeout = 5000)
    public void testPutIfAbsent1_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test(timeout = 10000)
    public void testReplace_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.replace(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test(timeout = 10000)
    public void testReplaceValue_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.replace(null, "A", "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test(timeout = 10000)
    public void testPutIfAbsent2_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.putIfAbsent(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test(timeout = 5000)
    public void testReplace2_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.replace(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test(timeout = 5000)
    public void testReplaceValue2_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.replace(notPresent, null, "A");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test(timeout = 5000)
    public void testReplaceValue3_NullPointerException() throws IOException {
        try (ChronicleMap c = newShmIntString(s_port++)) {
            c.replace(notPresent, "A", null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Test(timeout = 10000)
    public void testRemove1_NullPointerException() throws IOException {
        try (ChronicleMap c = newStrStrMap(s_port++)) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test(timeout = 5000)
    public void testRemove2_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newStrStrMap(s_port++)) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Test(timeout = 5000)
    public void testRemove3() throws IOException {

        try (ChronicleMap c = newStrStrMap(s_port++)) {
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

