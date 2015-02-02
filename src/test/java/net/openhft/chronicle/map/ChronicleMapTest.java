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

package net.openhft.chronicle.map;

import com.google.common.collect.HashBiMap;
import com.google.common.primitives.Ints;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.DataValueGenerator;
import net.openhft.lang.values.IntValue;
import net.openhft.lang.values.LongValue;
import net.openhft.lang.values.LongValue$$Native;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.map.Alignment.*;
import static net.openhft.chronicle.map.StatelessClientTest.localClient;
import static org.junit.Assert.*;

@SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
@Ignore
public class ChronicleMapTest {

    static long count = 0;
    private StringBuilder sb = new StringBuilder();

    static void assertKeySet(Set<Integer> keySet, int[] expectedKeys) {
        Set<Integer> expectedSet = new HashSet<Integer>();
        for (int expectedKey : expectedKeys) {
            expectedSet.add(expectedKey);
        }

        assertEquals(expectedSet, keySet);
    }

    static void assertValues(Collection<CharSequence> values, CharSequence[] expectedValues) {
        List<String> expectedList = new ArrayList<String>();
        for (CharSequence expectedValue : expectedValues) {
            expectedList.add(expectedValue.toString());
        }
        Collections.sort(expectedList);

        List<String> actualList = new ArrayList<String>();
        for (CharSequence actualValue : values) {
            actualList.add(actualValue.toString());
        }
        Collections.sort(actualList);

        assertEquals(expectedList, actualList);
    }

    static void assertEntrySet(Set<Map.Entry<Integer, CharSequence>> entrySet, int[] expectedKeys, CharSequence[] expectedValues) {
        Set<Map.Entry<Integer, CharSequence>> expectedSet = new HashSet<Map.Entry<Integer, CharSequence>>();
        for (int i = 0; i < expectedKeys.length; i++) {
            expectedSet.add(new AbstractMap.SimpleEntry<Integer, CharSequence>(expectedKeys[i], expectedValues[i]));
        }

        assertEquals(expectedSet, entrySet);
    }

    static void assertMap(Map<Integer, CharSequence> map, int[] expectedKeys, CharSequence[] expectedValues) {
        assertEquals(expectedKeys.length, map.size());
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals("On position " + i, expectedValues[i], map.get(expectedKeys[i]));
        }
    }

    public static LongValue nativeLongValue() {
        return new LongValue$$Native();
    }

    public static IntValue nativeIntValue() {
        return DataValueClasses.newDirectReference(IntValue.class);
//        return new LongValue$$Native();
    }

    static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/chm-test" + System.nanoTime() + count++);
        file.deleteOnExit();
        return file;
    }

    private ChronicleMap<Integer, CharSequence> getViewTestMap(int noOfElements) throws IOException {
        ChronicleMap<Integer, CharSequence> map =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .entries(noOfElements * 2 + 100)
                        .averageValueSize((noOfElements + "").length())
                        .putReturnsNull(true)
                        .removeReturnsNull(true).create();

        int[] expectedKeys = new int[noOfElements];
        String[] expectedValues = new String[noOfElements];
        for (int i = 1; i <= noOfElements; i++) {
            String value = "" + i;
            map.put(i, value);
            expectedKeys[i - 1] = i;
            expectedValues[i - 1] = value;
        }

        return map;

    }

    private static void printStatus() {
        if (!new File("/proc/self/status").exists()) return;
        try {
            BufferedReader br = new BufferedReader(new FileReader("/proc/self/status"));
            for (String line; (line = br.readLine()) != null; )
                if (line.startsWith("Vm"))
                    System.out.print(line.replaceAll("  +", " ") + ", ");

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRemoveWithKey() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder
                             .of(CharSequence.class, CharSequence.class)
                             .minSegments(2).create()) {

            assertFalse(map.containsKey("key3"));
            map.put("key1", "one");
            map.put("key2", "two");
            assertEquals(2, map.size());

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertFalse(map.containsKey("key3"));

            assertEquals("one", map.get("key1"));
            assertEquals("two", map.get("key2"));

            final CharSequence result = map.remove("key1");

            assertEquals(1, map.size());

            assertEquals("one", result);
            assertFalse(map.containsKey("key1"));

            assertEquals(null, map.get("key1"));
            assertEquals("two", map.get("key2"));
            assertFalse(map.containsKey("key3"));

            // lets add one more item for luck !
            map.put("key3", "three");
            assertEquals("three", map.get("key3"));
            assertTrue(map.containsKey("key3"));
            assertEquals(2, map.size());

            // and just for kicks we'll overwrite what we have
            map.put("key3", "overwritten");
            assertEquals("overwritten", map.get("key3"));
            assertTrue(map.containsKey("key3"));
            assertEquals(2, map.size());

        }
    }


    @Test
    public void testByteArrayPersistenceFileReuse() throws Exception {
        final File persistenceFile = Builder.getPersistenceFile();

        for (int i = 0; i < 2; i++) {
            try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[]
                    .class).createPersistedTo(persistenceFile)) {

                byte[] o = map.get("hello".getBytes());
                System.out.println(o == null ? "null" : new String(o));
                map.put("hello".getBytes(), "world".getBytes());
            }
        }

        persistenceFile.delete();


    }

    @Test
    public void testEqualsCharSequence() throws Exception {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class);

        try (final ChronicleMap<CharSequence, CharSequence> map1 = builder.create()) {

            map1.put("hello", "world");

            try (final ChronicleMap<CharSequence, CharSequence> map2 = builder.create()) {
                map2.put("hello", "world");

                assertEquals(map1, map2);
            }

        }
    }

    @Ignore("HCOLL-265 Chronicle Maps with Identical byte[] & char[] values are not equal")
    @Test
    public void testEqualsCharArray() throws Exception {

        char[] value = new char[5];
        Arrays.fill(value, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder.of(CharSequence.class, char[].class);

        try (final ChronicleMap<CharSequence, char[]> map1 = builder.create()) {

            map1.put("hello", value);

            try (final ChronicleMap<CharSequence, char[]> map2 = builder.create()) {
                map2.put("hello", value);

                assertEquals(map1, map2);
            }

        }
    }


    @Ignore("HCOLL-265 Chronicle Maps with Identical byte[] & char[] values are not equal")
    @Test
    public void testEqualsByteArray() throws Exception {

        byte[] value = new byte[5];
        Arrays.fill(value, (byte) 'X');

        ChronicleMapBuilder<CharSequence, byte[]> builder = ChronicleMapBuilder.of(CharSequence.class, byte[].class);

        try (final ChronicleMap<CharSequence, byte[]> map1 = builder.create()) {

            map1.put("hello", value);

            try (final ChronicleMap<CharSequence, byte[]> map2 = builder.create()) {
                map2.put("hello", value);

                assertEquals(map1, map2);
            }

        }
    }


    @Test
    public void testSize() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .minSegments(1024)
                             .removeReturnsNull(true).create()) {

            for (int i = 1; i < 1024; i++) {
                map.put("key" + i, "value");
                assertEquals(i, map.size());
            }

            for (int i = 1023; i >= 1; ) {
                map.remove("key" + i);
                i--;
                assertEquals(i, map.size());
            }
        }
    }

    @Test
    public void testRemoveInteger() throws IOException {

        int count = 300;
        try (final ChronicleMap<Object, Object> map = ChronicleMapBuilder.of(Object.class, Object
                .class)
                .entries(count)
                .minSegments(2).create()) {

            for (int i = 1; i < count; i++) {
                map.put(i, i);
                assertEquals(i, map.size());
            }

            for (int i = count - 1; i >= 1; ) {
                Integer j = (Integer) map.put(i, i);
                assertEquals(i, j.intValue());
                Integer j2 = (Integer) map.remove(i);
                assertEquals(i, j2.intValue());
                i--;
                assertEquals(i, map.size());
            }
        }
    }

    @Test
    public void testRemoveWithKeyAndRemoveReturnsNull() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .minSegments(2)
                             .removeReturnsNull(true).create()) {

            assertFalse(map.containsKey("key3"));
            map.put("key1", "one");
            map.put("key2", "two");
            assertEquals(2, map.size());

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertFalse(map.containsKey("key3"));

            assertEquals("one", map.get("key1"));
            assertEquals("two", map.get("key2"));

            final CharSequence result = map.remove("key1");
            assertEquals(null, result);

            assertEquals(1, map.size());

            assertFalse(map.containsKey("key1"));

            assertEquals(null, map.get("key1"));
            assertEquals("two", map.get("key2"));
            assertFalse(map.containsKey("key3"));

            // lets add one more item for luck !
            map.put("key3", "three");
            assertEquals("three", map.get("key3"));
            assertTrue(map.containsKey("key3"));
            assertEquals(2, map.size());

            // and just for kicks we'll overwrite what we have
            map.put("key3", "overwritten");
            assertEquals("overwritten", map.get("key3"));
            assertTrue(map.containsKey("key3"));
            assertEquals(2, map.size());

        }
    }

    @Test
    public void testReplaceWithKey() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .minSegments(2).create()) {

            map.put("key1", "one");
            map.put("key2", "two");
            assertEquals(2, map.size());

            assertEquals("one", map.get("key1"));
            assertEquals("two", map.get("key2"));

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));

            final CharSequence result = map.replace("key1", "newValue");

            assertEquals("one", result);
            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertEquals(2, map.size());

            assertEquals("newValue", map.get("key1"));
            assertEquals("two", map.get("key2"));

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertFalse(map.containsKey("key3"));

            assertEquals(2, map.size());

            // let and one more item for luck !
            map.put("key3", "three");
            assertEquals(3, map.size());

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertTrue(map.containsKey("key3"));
            assertEquals("three", map.get("key3"));

            // and just for kicks we'll overwrite what we have
            map.put("key3", "overwritten");
            assertEquals("overwritten", map.get("key3"));

            assertTrue(map.containsKey("key1"));
            assertTrue(map.containsKey("key2"));
            assertTrue(map.containsKey("key3"));

            final CharSequence result2 = map.replace("key2", "newValue");

            assertEquals("two", result2);
            assertEquals("newValue", map.get("key2"));

            final CharSequence result3 = map.replace("rubbish", "newValue");
            assertEquals(null, result3);

            assertFalse(map.containsKey("rubbish"));
            assertEquals(3, map.size());

        }
    }

    @Test
    public void testReplaceWithKeyAnd2Params() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .minSegments(2).create()) {

            map.put("key1", "one");
            map.put("key2", "two");

            assertEquals("one", map.get("key1"));
            assertEquals("two", map.get("key2"));

            final boolean result = map.replace("key1", "one", "newValue");

            assertEquals(true, result);

            assertEquals("newValue", map.get("key1"));
            assertEquals("two", map.get("key2"));

            // let and one more item for luck !
            map.put("key3", "three");
            assertEquals("three", map.get("key3"));

            // and just for kicks we'll overwrite what we have
            map.put("key3", "overwritten");
            assertEquals("overwritten", map.get("key3"));

            final boolean result2 = map.replace("key2", "two", "newValue2");

            assertEquals(true, result2);
            assertEquals("newValue2", map.get("key2"));

            final boolean result3 = map.replace("newKey", "", "newValue");
            assertEquals(false, result3);

            final boolean result4 = map.replace("key2", "newValue2", "newValue2");
            assertEquals(true, result4);

        }
    }

    @Test
    public void testRemoveWithKeyAndValue() throws Exception {

        try (final ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .minSegments(2).create()) {

            map.put("key1", "one");
            map.put("key2", "two");

            assertEquals("one", map.get("key1"));
            assertEquals("two", map.get("key2"));

            // a false remove
            final boolean wasRemoved1 = map.remove("key1", "three");

            assertFalse(wasRemoved1);

            assertEquals(null, map.get("key1"), "one");
            assertEquals("two", map.get("key2"), "two");

            map.put("key1", "one");

            final boolean wasRemoved2 = map.remove("key1", "three");
            assertFalse(wasRemoved2);

            // lets add one more item for luck !
            map.put("key3", "three");
            assertEquals("three", map.get("key3"));

            // and just for kicks we'll overwrite what we have
            map.put("key3", "overwritten");
            assertEquals("overwritten", map.get("key3"));

        }
    }

    @Test
    public void testAcquireWithNullContainer() throws Exception {
        try (ChronicleMap<CharSequence, LongValue> map =
                     ChronicleMapBuilder.of(CharSequence.class, LongValue.class)
                             .entries(1000)
                             .entryAndValueAlignment(OF_4_BYTES)
                             .create()) {
            map.acquireUsing("key", new LongValue$$Native());
            assertEquals(0, map.acquireUsing("key", null).getValue());
        }
    }

    // i7-3970X CPU @ 3.50GHz, hex core: -verbose:gc -Xmx64m
    // to tmpfs file system
    // 10M users, updated 12 times. Throughput 19.3 M ops/sec, no GC!
    // 50M users, updated 12 times. Throughput 19.8 M ops/sec, no GC!
    // 100M users, updated 12 times. Throughput 19.0M ops/sec, no GC!
    // 200M users, updated 12 times. Throughput 18.4 M ops/sec, no GC!
    // 400M users, updated 12 times. Throughput 18.4 M ops/sec, no GC!

    // to ext4 file system.
    // 10M users, updated 12 times. Throughput 17.7 M ops/sec, no GC!
    // 50M users, updated 12 times. Throughput 16.5 M ops/sec, no GC!
    // 100M users, updated 12 times. Throughput 15.9 M ops/sec, no GC!
    // 200M users, updated 12 times. Throughput 15.4 M ops/sec, no GC!
    // 400M users, updated 12 times. Throughput 7.8 M ops/sec, no GC!
    // 600M users, updated 12 times. Throughput 5.8 M ops/sec, no GC!

    // dual E5-2650v2 @ 2.6 GHz, 128 GB: -verbose:gc -Xmx32m
    // to tmpfs
    // TODO small GC on startup should be tidied up, [GC 9216K->1886K(31744K), 0.0036750 secs]
    // 10M users, updated 16 times. Throughput 33.0M ops/sec, VmPeak: 5373848 kB, VmRSS: 544252 kB
    // 50M users, updated 16 times. Throughput 31.2 M ops/sec, VmPeak: 9091804 kB, VmRSS: 3324732 kB
    // 250M users, updated 16 times. Throughput 30.0 M ops/sec, VmPeak:	24807836 kB, VmRSS: 14329112 kB
    // 1000M users, updated 16 times, Throughput 24.1 M ops/sec, VmPeak: 85312732 kB, VmRSS: 57165952 kB
    // 2500M users, updated 16 times, Throughput 23.5 M ops/sec, VmPeak: 189545308 kB, VmRSS: 126055868 kB

    // to ext4
    // 10M users, updated 16 times. Throughput 28.4 M ops/sec, VmPeak: 5438652 kB, VmRSS: 544624 kB
    // 50M users, updated 16 times. Throughput 28.2 M ops/sec, VmPeak: 9091804 kB, VmRSS: 9091804 kB
    // 250M users, updated 16 times. Throughput 26.1 M ops/sec, VmPeak:	24807836 kB, VmRSS: 24807836 kB
    // 1000M users, updated 16 times, Throughput 1.3 M ops/sec, TODO FIX this

    @Test
    public void testGetWithNullContainer() throws Exception {
        try (ChronicleMap<CharSequence, LongValue> map =
                     ChronicleMapBuilder.of(CharSequence.class, LongValue.class)
                             .entries(10)
                             .entryAndValueAlignment(OF_4_BYTES)
                             .create()) {
            map.acquireUsing("key", new LongValue$$Native());
            assertEquals(0, map.getUsing("key", null).getValue());

        }
    }

    @Test
    public void testGetWithoutAcquireFirst() throws Exception {
        try (ChronicleMap<CharSequence, LongValue> map =
                     ChronicleMapBuilder.of(CharSequence.class, LongValue.class)
                             .entries(10)
                             .entryAndValueAlignment(OF_4_BYTES)
                             .create()) {
            assertNull(map.getUsing("key", DataValueClasses.newDirectReference(LongValue.class)));

        }
    }

    //  i7-3970X CPU @ 3.50GHz, hex core: -Xmx30g -verbose:gc
    // 10M users, updated 12 times. Throughput 16.2 M ops/sec, longest [Full GC 853669K->852546K(3239936K), 0.8255960 secs]
    // 50M users, updated 12 times. Throughput 13.3 M ops/sec,  longest [Full GC 5516214K->5511353K(13084544K), 3.5752970 secs]
    // 100M users, updated 12 times. Throughput 11.8 M ops/sec, longest [Full GC 11240703K->11233711K(19170432K), 5.8783010 secs]
    // 200M users, updated 12 times. Throughput 4.2 M ops/sec, longest [Full GC 25974721K->22897189K(27962048K), 21.7962600 secs]

    // dual E5-2650v2 @ 2.6 GHz, 128 GB: -verbose:gc -Xmx100g
    // 10M users, updated 16 times. Throughput 155.3 M ops/sec, VmPeak: 113291428 kB, VmRSS: 9272176 kB, [Full GC 1624336K->1616457K(7299072K), 2.5381610 secs]
    // 50M users, updated 16 times. Throughput 120.4 M ops/sec, VmPeak: 113291428 kB, VmRSS: 28436248 kB [Full GC 6545332K->6529639K(18179584K), 6.9053810 secs]
    // 250M users, updated 16 times. Throughput 114.1 M ops/sec, VmPeak: 113291428 kB, VmRSS: 76441464 kB  [Full GC 41349527K->41304543K(75585024K), 17.3217490 secs]
    // 1000M users, OutOfMemoryError.

    @Test
    public void testAcquireAndGet() throws IOException, ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        int entries = 3/*00 * 1000*/;
        try (ChronicleMap<CharSequence, LongValue> map2 = ChronicleMapBuilder.of(CharSequence.class,
                LongValue.class)
                .entries((long) entries)
                .minSegments(1)
                .averageKeySize(10)
                .entryAndValueAlignment(OF_4_BYTES)
                .create()) {
            LongValue value4 = new LongValue$$Native();
            LongValue value22 = new LongValue$$Native();
            LongValue value32 = new LongValue$$Native();

            for (int j2 = 1; j2 <= 3; j2++) {
                for (int i2 = 0; i2 < entries; i2++) {
                    CharSequence userCS2 = getUserCharSequence(i2);

                    if (j2 > 1) {
                        assertNotNull(userCS2.toString(), map2.getUsing(userCS2, value4));
                    } else {
                        map2.acquireUsing(userCS2, value4);
                    }
                    if (i2 >= 1)
                        assertTrue(userCS2.toString(), map2.containsKey(getUserCharSequence(1)));
                    assertEquals(userCS2.toString(), j2 - 1, value4.getValue());

                    value4.addAtomicValue(1);

                    assertEquals(value22, map2.acquireUsing(userCS2, value22));
                    assertEquals(j2, value22.getValue());

                    assertEquals(value32, map2.getUsing(userCS2, value32));
                    assertEquals(j2, value32.getValue());
                }
            }

            try (ChronicleMap<CharSequence, LongValue> map1 = ChronicleMapBuilder.of(CharSequence.class,
                    LongValue.class)
                    .entries((long) entries)
                    .minSegments(1)
                    .averageKeySize(10)
                    .entryAndValueAlignment(NO_ALIGNMENT)
                    .create()) {
                LongValue value1 = new LongValue$$Native();
                LongValue value21 = new LongValue$$Native();
                LongValue value31 = new LongValue$$Native();

                for (int j1 = 1; j1 <= 3; j1++) {
                    for (int i1 = 0; i1 < entries; i1++) {
                        CharSequence userCS1 = getUserCharSequence(i1);

                        if (j1 > 1) {
                            assertNotNull(userCS1.toString(), map1.getUsing(userCS1, value1));
                        } else {
                            map1.acquireUsing(userCS1, value1);
                        }
                        if (i1 >= 1)
                            assertTrue(userCS1.toString(), map1.containsKey(getUserCharSequence(1)));
                        assertEquals(userCS1.toString(), j1 - 1, value1.getValue());

                        value1.addAtomicValue(1);

                        assertEquals(value21, map1.acquireUsing(userCS1, value21));
                        assertEquals(j1, value21.getValue());

                        assertEquals(value31, map1.getUsing(userCS1, value31));
                        assertEquals(j1, value31.getValue());
                    }
                }
            }
            try (ChronicleMap<CharSequence, LongValue> map = ChronicleMapBuilder.of(CharSequence
                    .class, LongValue.class)
                    .entries((long) entries)
                    .minSegments(1)
                    .averageKeySize(10)
                    .entryAndValueAlignment(OF_8_BYTES)
                    .create()) {
                LongValue value = new LongValue$$Native();
                LongValue value2 = new LongValue$$Native();
                LongValue value3 = new LongValue$$Native();

                for (int j = 1; j <= 3; j++) {
                    for (int i = 0; i < entries; i++) {
                        CharSequence userCS = getUserCharSequence(i);

                        if (j > 1) {
                            assertNotNull(userCS.toString(), map.getUsing(userCS, value));
                        } else {
                            map.acquireUsing(userCS, value);
                        }
                        if (i >= 1)
                            assertTrue(userCS.toString(), map.containsKey(getUserCharSequence(1)));
                        assertEquals(userCS.toString(), j - 1, value.getValue());

                        value.addAtomicValue(1);

                        assertEquals(value2, map.acquireUsing(userCS, value2));
                        assertEquals(j, value2.getValue());

                        assertEquals(value3, map.getUsing(userCS, value3));
                        assertEquals(j, value3.getValue());
                    }
                }

            }
        }
    }


    @Ignore("Possible candidate for TC JVM crashes")
    @Test
    public void testAcquireFromMultipleThreads() throws Exception {
        int entries = 1000 * 1000;
        try (ChronicleMap<CharSequence, LongValue> map2 = ChronicleMapBuilder.of(CharSequence.class,
                LongValue.class)
                .entries((long) entries)
                .minSegments(128)
                .averageKeySize(10)
                .entryAndValueAlignment(NO_ALIGNMENT)
                .create()) {

            CharSequence key2 = getUserCharSequence(0);
            map2.acquireUsing(key2, new LongValue$$Native());

            int iterations2 = 10000;
            int noOfThreads2 = 10;
            CyclicBarrier barrier2 = new CyclicBarrier(noOfThreads2);

            Thread[] threads2 = new Thread[noOfThreads2];
            for (int t2 = 0; t2 < noOfThreads2; t2++) {
                threads2[t2] = new Thread(new IncrementRunnable(map2, key2, iterations2, barrier2));
                threads2[t2].start();
            }
            for (int t2 = 0; t2 < noOfThreads2; t2++) {
                threads2[t2].join();
            }

            assertEquals(noOfThreads2 * iterations2,
                    map2.acquireUsing(key2, new LongValue$$Native()).getValue());

            try (ChronicleMap<CharSequence, LongValue> map1 = ChronicleMapBuilder.of(CharSequence
                    .class, LongValue.class)
                    .entries((long) entries)
                    .minSegments(128)
                    .averageKeySize(10)
                    .entryAndValueAlignment(OF_4_BYTES)
                    .create()) {

                CharSequence key1 = getUserCharSequence(0);
                map1.acquireUsing(key1, new LongValue$$Native());

                int iterations1 = 10000;
                int noOfThreads1 = 10;
                CyclicBarrier barrier1 = new CyclicBarrier(noOfThreads1);

                Thread[] threads1 = new Thread[noOfThreads1];
                for (int t1 = 0; t1 < noOfThreads1; t1++) {
                    threads1[t1] = new Thread(new IncrementRunnable(map1, key1, iterations1, barrier1));
                    threads1[t1].start();
                }
                for (int t1 = 0; t1 < noOfThreads1; t1++) {
                    threads1[t1].join();
                }

                assertEquals(noOfThreads1 * iterations1,
                        map1.acquireUsing(key1, new LongValue$$Native()).getValue());

                try (ChronicleMap<CharSequence, LongValue> map = ChronicleMapBuilder.of(CharSequence
                        .class, LongValue.class)
                        .entries((long) entries)
                        .minSegments(128)
                        .averageKeySize(10)
                        .entryAndValueAlignment(OF_8_BYTES)
                        .create()) {

                    CharSequence key = getUserCharSequence(0);
                    map.acquireUsing(key, new LongValue$$Native());

                    int iterations = 10000;
                    int noOfThreads = 10;
                    CyclicBarrier barrier = new CyclicBarrier(noOfThreads);

                    Thread[] threads = new Thread[noOfThreads];
                    for (int t = 0; t < noOfThreads; t++) {
                        threads[t] = new Thread(new IncrementRunnable(map, key, iterations, barrier));
                        threads[t].start();
                    }
                    for (int t = 0; t < noOfThreads; t++) {
                        threads[t].join();
                    }

                    assertEquals(noOfThreads * iterations,
                            map.acquireUsing(key, new LongValue$$Native()).getValue());

                }
            }
        }

    }

    @Test
    public void testIntValue() {
        DataValueGenerator dvg = new DataValueGenerator();
        dvg.setDumpCode(true);
        dvg.acquireNativeClass(IntValue.class);
        dvg.acquireNativeClass(LongValue.class);
    }

    @Test
    public void testLargerEntries() {
        for (int segments : new int[]{128, 256, 512, 1024}) {
            int entries = 1000000, entrySize = 512;
            ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                    .of(CharSequence.class, CharSequence.class)
                    .entries(entries * 11 / 10)
                    .actualSegments(segments)
                    .averageKeySize(14)
                    .averageValueSize(entrySize - 14 - 2);
            try (ChronicleMap<CharSequence, CharSequence> map = builder.create()) {

                StringBuilder sb = new StringBuilder();
                while (sb.length() < entrySize - 14 - 2)
                    sb.append('+');
                for (int i = 0; i < entries; i++) {
                    map.put("us:" + i, sb);
                }
            }
        }
    }

    @Test
    @Ignore("Performance test")
    public void testAcquirePerf256()
            throws IOException, ClassNotFoundException, IllegalAccessException,
            InstantiationException, InterruptedException, ExecutionException {
//        int runs = Integer.getInteger("runs", 10);
        int procs = 1; // Runtime.getRuntime().availableProcessors();
        int threads = procs * 3;
        for (int runs : new int[]{1, /*10, 250, 500, 1000, 2500*/}) {
            for (int entrySize : new int[]{240, 256}) {
                int valuePadding = entrySize - 16;
                char[] chars = new char[valuePadding];
                Arrays.fill(chars, 'x');
                final StringBuilder value0 = new StringBuilder();
                value0.append(chars);
                File file;
                for (int segments : new int[]{/*128, 256, */512/*, 1024, 2048*/}) {
                    final long entries = runs * 1000 * 1000L;
                    ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                            .of(CharSequence.class, CharSequence.class)
                            .entries(entries)
                            .actualSegments(segments)
                            .averageKeySize(14)
                            .averageValueSize(value0.length() + 4);


//                    File tmpFile = File.createTempFile("testAcquirePerf", ".deleteme");
//                    tmpFile.deleteOnExit();
                    //createPersistedTo(tmpFile);
                    try (final ChronicleMap<CharSequence, CharSequence> map = builder.create()) {
                        file = map.file();
                        int count = runs > 500 ? 2 : 3;
                        System.out.println("\nKey size: " + runs + " Million entries. " + builder);
                        for (int j = 0; j < count; j++) {
                            long start = System.currentTimeMillis();
                            for (int i = 0; i < threads; i++) {
                                final int t = i;

                                Random rand = new Random(t);
                                StringBuilder key = new StringBuilder();
                                StringBuilder value = new StringBuilder();
                                long next = 50 * 1000 * 1000;
                                // use a factor to give up to 10 digit numbers.
                                int factor = Math.max(1,
                                        (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                                for (long k = t; k < entries; k++) {
                                    key.setLength(0);
                                    key.append("us:");
                                    key.append(k * factor);
                                    // 75% reads, 25% writes.
                                    if (rand.nextInt(4) > 0) {
                                        map.getUsing(key, value);

                                    } else {
                                        try (WriteContext wc = map.acquireUsingLocked(key, value)) {
                                            if (value.length() < value0.length() - 1)
                                                value.append(value0);
                                            else if (value.length() > value0.length())
                                                value.setLength(value0.length() - 1);
                                            else
                                                value.append('+');
                                        }
                                    }
                                    if (t == 0 && k >= next) {
                                        long size = map.longSize();
                                        if (size < 0) throw new AssertionError("size: " + size);
                                        System.out.println(k + ", size: " + size);
                                        next += 50 * 1000 * 1000;
                                    }
                                }
                            }

                            long time = System.currentTimeMillis() - start;
                            System.out.printf("EntrySize: %,d Entries: %,d M Segments: " +
                                            "%,d Throughput %.1f M ops/sec%n",
                                    entrySize, runs, segments,
                                    threads * entries / 1000.0 / time);
                        }
                        printStatus();

                    }

                    if (file != null)
                        file.delete();
                }
            }
        }
    }

    @Test
    @Ignore("Performance test")
    public void testAcquirePerf()
            throws IOException, ClassNotFoundException, IllegalAccessException,
            InstantiationException, InterruptedException, ExecutionException {
//        int runs = Integer.getInteger("runs", 10);
        int procs = 1; // Runtime.getRuntime().availableProcessors();
        int threads = procs * 3;
        File file;
        ExecutorService es = Executors.newFixedThreadPool(procs);
        for (int runs : new int[]{1, /*10, 250, 500, 1000, 2500*/}) {
            for (int entrySize : new int[]{240, 256}) {
                int valuePadding = entrySize - 16;
                char[] chars = new char[valuePadding];
                Arrays.fill(chars, 'x');
                final StringBuilder value0 = new StringBuilder();
                value0.append(chars);

                for (int segments : new int[]{/*128, 256, */512/*, 1024, 2048*/}) {
                    final long entries = runs * 1000 * 1000L;
                    ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                            .of(CharSequence.class, CharSequence.class)
                            .entries(entries)
                            .actualSegments(segments)
                            .averageKeySize(14)
                            .averageValueSize(value0.length() + 4);

//                    File tmpFile = File.createTempFile("testAcquirePerf", ".deleteme");
//                    tmpFile.deleteOnExit();
                    try (final ChronicleMap<CharSequence, CharSequence> map = builder.create()) { //createPersistedTo(tmpFile);
                        file = map.file();
                        int count = runs > 500 ? 2 : 3;
                        final int independence = Math.min(procs, runs > 500 ? 8 : 4);
                        System.out.println("\nKey size: " + runs + " Million entries. " + builder);
                        for (int j = 0; j < count; j++) {
                            long start = System.currentTimeMillis();
                            List<Future> futures = new ArrayList<>();
                            for (int i = 0; i < threads; i++) {
                                final int t = i;
                                futures.add(es.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        Random rand = new Random(t);
                                        StringBuilder key = new StringBuilder();
                                        StringBuilder value = new StringBuilder();
                                        long next = 50 * 1000 * 1000;
                                        // use a factor to give up to 10 digit numbers.
                                        int factor = Math.max(1,
                                                (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                                        for (long j = t % independence; j < entries + independence - 1;
                                             j += independence) {
                                            key.setLength(0);
                                            key.append("us:");
                                            key.append(j * factor);
                                            // 75% reads, 25% writes.
                                            if (rand.nextInt(4) > 0) {
                                                map.getUsing(key, value);

                                            } else {
                                                try (WriteContext wc = map.acquireUsingLocked(key, value)) {
                                                    if (value.length() < value0.length() - 1)
                                                        value.append(value0);
                                                    else if (value.length() > value0.length())
                                                        value.setLength(value0.length() - 1);
                                                    else
                                                        value.append('+');
                                                }
                                            }
                                            if (t == 0 && j >= next) {
                                                long size = map.longSize();
                                                if (size < 0)
                                                    throw new AssertionError("size: " + size);
                                                System.out.println(j + ", size: " + size);
                                                next += 50 * 1000 * 1000;
                                            }
                                        }
                                    }
                                }));
                            }
                            for (Future future : futures) {
                                future.get();
                            }
                            long time = System.currentTimeMillis() - start;
                            System.out.printf("EntrySize: %,d Entries: %,d M Segments: %,d Throughput %.1f M ops/sec%n",
                                    entrySize, runs, segments,
                                    threads * entries / independence / 1000.0 / time);
                        }
                        printStatus();

                    }
                    if (file != null)
                        file.delete();
                }
            }
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    @Ignore("Performance test")
    public void testAcquireLockedPerf()
            throws IOException, ClassNotFoundException, IllegalAccessException,
            InstantiationException, InterruptedException, ExecutionException {
//        int runs = Integer.getInteger("runs", 10);
        int procs = Runtime.getRuntime().availableProcessors();
        if (procs > 8) procs--;
        int threads = procs * 3;
        ExecutorService es = Executors.newFixedThreadPool(procs);
        for (int runs : new int[]{1, 2, 5, 10, 25, 50, 100, 500, 1000, 2500}) {

            final long entries = runs * 1000 * 1000L;
            ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                    .of(CharSequence.class, LongValue.class)
                    .entries(entries)
                    .entryAndValueAlignment(OF_8_BYTES)
                    .actualSegments(256)
                    .averageKeySize(13);

            File tmpFile = File.createTempFile("testAcquirePerf", ".deleteme");
            tmpFile.deleteOnExit();
            try (final ChronicleMap<CharSequence, LongValue> map = builder.createPersistedTo
                    (tmpFile)) {

                int count = runs >= 5 ? 2 : 3;
                final int independence = Math.min(procs, runs > 500 ? 8 : 4);
                System.out.println("\nKey size: " + runs + " Million entries. " + builder);
                for (int j = 0; j < count; j++) {
                    long start = System.currentTimeMillis();
                    List<Future> futures = new ArrayList<Future>();
                    for (int i = 0; i < threads; i++) {
                        final int t = i;
                        futures.add(es.submit(new Runnable() {
                            @Override
                            public void run() {
                                LongValue value = nativeLongValue();
                                StringBuilder sb = new StringBuilder();
                                long next = 50 * 1000 * 1000;
                                Random rand = new Random();
                                // use a factor to give up to 10 digit numbers.
                                int factor = Math.max(1,
                                        (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                                for (long j = t % independence; j < entries + independence - 1;
                                     j += independence) {
                                    sb.setLength(0);
                                    sb.append("us:");
                                    sb.append(j * factor);
                                    long n;
                                    // 75% read
                                    if (rand.nextBoolean() || rand.nextBoolean()) {
                                        try (ReadContext rc = map.getUsingLocked(sb, value)) {
                                            n = rc.present() ? value.getValue() + 1 : 1;
                                        }
                                    } else {
                                        try (WriteContext wc = map.acquireUsingLocked(sb, value)) {
                                            n = value.addValue(1);
                                        }
                                    }
                                    assert n > 0 && n < 1000 : "Counter corrupted " + n;
                                    if (t == 0 && j >= next) {
                                        long size = map.longSize();
                                        if (size < 0) throw new AssertionError("size: " + size);
                                        System.out.println(j + ", size: " + size);
                                        next += 50 * 1000 * 1000;
                                    }
                                }
                            }
                        }));
                    }
                    for (Future future : futures) {
                        future.get();
                    }
                    long time = System.currentTimeMillis() - start;
                    System.out.printf("Throughput %.1f M ops/sec%n",
                            threads * entries / independence / 1000.0 / time);
                }
            }
            printStatus();


            tmpFile.delete();
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    @Ignore("Performance test")
    public void testAcquireLockedLLPerf()
            throws IOException, ClassNotFoundException, IllegalAccessException,
            InstantiationException, InterruptedException, ExecutionException {
//        int runs = Integer.getInteger("runs", 10);
        int procs = Runtime.getRuntime().availableProcessors();
        int threads = procs * 3; // runs > 100 ? procs / 2 : procs;
        ExecutorService es = Executors.newFixedThreadPool(procs);
        for (int runs : new int[]{10, 50, 100, 250, 500, 1000, 2500}) {
// JAVA 8 produces more garbage than previous versions for internal work.
//            System.gc();
            final long entries = runs * 1000 * 1000L;
            ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMapBuilder
                    .of(LongValue.class, LongValue.class)
                    .entries(entries)
                    .actualSegments(8 * 1024);

            File tmpFile = File.createTempFile("testAcquirePerf", ".deleteme");
            tmpFile.deleteOnExit();
            try (final ChronicleMap<LongValue, LongValue> map = builder.createPersistedTo(tmpFile)) {

                int count = runs > 500 ? runs > 1200 ? 3 : 5 : 5;
                final int independence = Math.min(procs, runs > 500 ? 8 : 4);
                System.out.println("\nKey size: " + runs + " Million entries. " + builder);
                for (int j = 0; j < count; j++) {
                    long start = System.currentTimeMillis();
                    List<Future> futures = new ArrayList<Future>();
                    for (int i = 0; i < threads; i++) {
                        final int t = i;
                        futures.add(es.submit(new Runnable() {
                            @Override
                            public void run() {
                                LongValue key = DataValueClasses.newDirectInstance(LongValue.class);
                                LongValue value = nativeLongValue();
                                long next = 50 * 1000 * 1000;
                                // use a factor to give up to 10 digit numbers.
                                int factor = Math.max(1,
                                        (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                                for (long j = t % independence; j < entries + independence - 1;
                                     j += independence) {
                                    key.setValue(j * factor);
                                    long n;
                                    try (WriteContext wc = map.acquireUsingLocked(key, value)) {
                                        n = value.addValue(1);
                                    }
                                    assert n > 0 && n < 1000 : "Counter corrupted " + n;
                                    if (t == 0 && j >= next) {
                                        long size = map.longSize();
                                        if (size < 0) throw new AssertionError("size: " + size);
                                        System.out.println(j + ", size: " + size);
                                        next += 50 * 1000 * 1000;
                                    }
                                }
                            }
                        }));
                    }
                    for (Future future : futures) {
                        future.get();
                    }
                    long time = System.currentTimeMillis() - start;
                    System.out.printf("Throughput %.1f M ops/sec%n",
                            threads * entries / independence / 1000.0 / time);
                }
            }
            printStatus();

            tmpFile.delete();
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    @Ignore("Performance test")
    public void testCHMAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        for (int runs : new int[]{10, 50, 250, 500, 1000, 2500}) {
            System.out.println("Testing " + runs + " million entries");
            final long entries = runs * 1000 * 1000L;

            int procs = Runtime.getRuntime().availableProcessors();
            int threads = procs * 2;
            int count = runs > 500 ? runs > 1200 ? 1 : 2 : 5;
            final int independence = Math.min(procs, runs > 500 ? 8 : 4);
            for (int j = 0; j < count; j++) {
                final ConcurrentMap<String, AtomicInteger> map = new ConcurrentHashMap<String, AtomicInteger>((int) (entries * 5 / 4), 1.0f, 1024);
                long start = System.currentTimeMillis();
                ExecutorService es = Executors.newFixedThreadPool(procs);
                for (int i = 0; i < threads; i++) {
                    final int t = i;
                    es.submit(new Runnable() {
                        @Override
                        public void run() {
                            StringBuilder sb = new StringBuilder();
                            int next = 50 * 1000 * 1000;
                            // use a factor to give up to 10 digit numbers.
                            int factor = Math.max(1, (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                            for (long i = t % independence; i < entries; i += independence) {
                                sb.setLength(0);
                                sb.append("u:");
                                sb.append(i * factor);
                                String key = sb.toString();
                                AtomicInteger count = map.get(key);
                                if (count == null) {
                                    map.putIfAbsent(key, new AtomicInteger());
                                    count = map.get(key);
                                }
                                count.getAndIncrement();
                                if (t == 0 && i == next) {
                                    System.out.println(i);
                                    next += 50 * 1000 * 1000;
                                }
                            }
                        }
                    });
                }
                es.shutdown();
                es.awaitTermination(10, TimeUnit.MINUTES);
                printStatus();
                long time = System.currentTimeMillis() - start;
                System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / 1000.0 / time);
            }
        }
    }

    private CharSequence getUserCharSequence(int i) {
        sb.setLength(0);
        sb.append("u:");
        sb.append(i * 9876); // test 10 digit user numbers.
        return sb;
    }

    @Test
    public void testPutAndRemove() throws IOException, ClassNotFoundException,
            IllegalAccessException, InstantiationException {

        int entries = 100 * 1000;
        try (ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .entries(entries)
                             .minSegments(16)
                             .averageKeySize("user:".length() + 6)
                             .averageValueSize("value:".length() + 6)
                             .putReturnsNull(true)
                             .removeReturnsNull(true).create()) {
            StringBuilder key = new StringBuilder();
            StringBuilder value = new StringBuilder();
            StringBuilder value2 = new StringBuilder();
            for (int j = 1; j <= 3; j++) {
                for (int i = 0; i < entries; i++) {
                    key.setLength(0);
                    key.append("user:").append(i);
                    value.setLength(0);
                    value.append("value:").append(i);
//                System.out.println(key);
                    assertNull(map.getUsing(key, value));
                    assertNull(map.put(key, value));
                    assertNotNull(map.getUsing(key, value2));
                    assertEquals(value.toString(), value2.toString());
                    assertNull(map.remove(key));
                    assertNull(map.getUsing(key, value));
                }
            }

        }
    }

    @Test
    public void mapRemoveReflectedInViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3);) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            map.remove(2);
            assertMap(map, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertEntrySet(entrySet, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertEntrySet(map.entrySet(), new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertKeySet(keySet, new int[]{1, 3});
            assertKeySet(map.keySet(), new int[]{1, 3});
            assertValues(values, new CharSequence[]{"1", "3"});
            assertValues(map.values(), new CharSequence[]{"1", "3"});
        }

    }


    @Test
    public void mapPutReflectedInViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            map.put(4, "4");
            assertMap(map, new int[]{4, 2, 3, 1}, new CharSequence[]{"4", "2", "3", "1"});
            assertEntrySet(entrySet, new int[]{4, 2, 3, 1}, new CharSequence[]{"4", "2", "3", "1"});
            assertEntrySet(map.entrySet(), new int[]{4, 2, 3, 1}, new CharSequence[]{"4", "2", "3", "1"});
            assertKeySet(keySet, new int[]{4, 2, 3, 1});
            assertKeySet(map.keySet(), new int[]{4, 2, 3, 1});
            assertValues(values, new CharSequence[]{"2", "1", "4", "3"});
            assertValues(map.values(), new CharSequence[]{"2", "1", "4", "3"});
        }
    }


    @Test
    public void entrySetRemoveReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            entrySet.remove(new AbstractMap.SimpleEntry<Integer, CharSequence>(2, "2"));
            assertMap(map, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertEntrySet(entrySet, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertKeySet(keySet, new int[]{1, 3});
            assertValues(values, new CharSequence[]{"1", "3"});
        }
    }


    @Test
    public void keySetRemoveReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            keySet.remove(2);
            assertMap(map, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertEntrySet(entrySet, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertKeySet(keySet, new int[]{1, 3});

            assertValues(values, new CharSequence[]{"1", "3"});

        }

    }

    @Test
    public void valuesRemoveReflectedInMap() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            values.remove("2");
            assertMap(map, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertEntrySet(entrySet, new int[]{1, 3}, new CharSequence[]{"1", "3"});
            assertKeySet(keySet, new int[]{1, 3});
            assertValues(values, new CharSequence[]{"1", "3"});

        }

    }

    @Test
    public void entrySetIteratorRemoveReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Map<Integer, CharSequence> refMap = new HashMap(map);
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            Iterator<Map.Entry<Integer, CharSequence>> entryIterator = entrySet.iterator();
            entryIterator.next();
            refMap.remove(entryIterator.next().getKey());
            entryIterator.remove();
            int[] expectedKeys = Ints.toArray(refMap.keySet());
            CharSequence[] expectedValues = refMap.values().toArray(new CharSequence[0]);
            assertMap(map, expectedKeys, expectedValues);
            assertEntrySet(entrySet, expectedKeys, expectedValues);
            assertKeySet(keySet, expectedKeys);
            assertValues(values, expectedValues);
        }
    }


    @Test
    public void keySetIteratorRemoveReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Map<Integer, CharSequence> refMap = new HashMap(map);
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            Iterator<Integer> keyIterator = keySet.iterator();
            keyIterator.next();
            refMap.remove(keyIterator.next());
            keyIterator.remove();
            int[] expectedKeys = Ints.toArray(refMap.keySet());
            CharSequence[] expectedValues = refMap.values().toArray(new CharSequence[0]);
            assertMap(map, expectedKeys, expectedValues);
            assertEntrySet(entrySet, expectedKeys, expectedValues);
            assertKeySet(keySet, expectedKeys);
            assertValues(values, expectedValues);

        }

    }

    @Test
    public void valuesIteratorRemoveReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            HashBiMap<Integer, CharSequence> refMap = HashBiMap.create(map);

            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            Iterator<CharSequence> valueIterator = values.iterator();
            valueIterator.next();
            refMap.inverse().remove(valueIterator.next());
            valueIterator.remove();
            int[] expectedKeys = Ints.toArray(refMap.keySet());
            CharSequence[] expectedValues = new CharSequence[expectedKeys.length];
            for (int i = 0; i < expectedKeys.length; i++) {
                expectedValues[i] = refMap.get(expectedKeys[i]);
            }
            assertMap(map, expectedKeys, expectedValues);
            assertEntrySet(entrySet, expectedKeys, expectedValues);
            assertKeySet(keySet, expectedKeys);
            assertValues(values, expectedValues);

        }

    }

    @Test
    public void entrySetRemoveAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            entrySet.removeAll(
                    Arrays.asList(
                            new AbstractMap.SimpleEntry<Integer, CharSequence>(1, "1"),
                            new AbstractMap.SimpleEntry<Integer, CharSequence>(2, "2")
                    )
            );
            assertMap(map, new int[]{3}, new CharSequence[]{"3"});
            assertEntrySet(entrySet, new int[]{3}, new CharSequence[]{"3"});
            assertKeySet(keySet, new int[]{3});
            assertValues(values, new CharSequence[]{"3"});

        }

    }

    @Test
    public void keySetRemoveAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            keySet.removeAll(Arrays.asList(1, 2));
            assertMap(map, new int[]{3}, new CharSequence[]{"3"});
            assertEntrySet(entrySet, new int[]{3}, new CharSequence[]{"3"});
            assertKeySet(keySet, new int[]{3});
            assertValues(values, new CharSequence[]{"3"});

        }

    }

    @Test
    public void valuesRemoveAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            values.removeAll(Arrays.asList("1", "2"));
            assertMap(map, new int[]{3}, new CharSequence[]{"3"});
            assertEntrySet(entrySet, new int[]{3}, new CharSequence[]{"3"});
            assertKeySet(keySet, new int[]{3});
            assertValues(values, new CharSequence[]{"3"});

        }
    }


    @Test
    public void entrySetRetainAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            entrySet.retainAll(
                    Arrays.asList(
                            new AbstractMap.SimpleEntry<Integer, CharSequence>(1, "1"),
                            new AbstractMap.SimpleEntry<Integer, CharSequence>(2, "2")
                    )
            );
            assertMap(map, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertEntrySet(entrySet, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertKeySet(keySet, new int[]{2, 1});
            assertValues(values, new CharSequence[]{"2", "1"});

        }
    }


    @Test
    public void keySetRetainAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            keySet.retainAll(Arrays.asList(1, 2));
            assertMap(map, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertEntrySet(entrySet, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertKeySet(keySet, new int[]{2, 1});
            assertValues(values, new CharSequence[]{"2", "1"});

        }

    }

    @Test
    public void valuesRetainAllReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            values.retainAll(Arrays.asList("1", "2"));
            assertMap(map, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertEntrySet(entrySet, new int[]{2, 1}, new CharSequence[]{"2", "1"});
            assertKeySet(keySet, new int[]{2, 1});
            assertValues(values, new CharSequence[]{"2", "1"});

        }
    }

    @Test
    public void entrySetClearReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            entrySet.clear();
            org.junit.Assert.assertTrue(map.isEmpty());
            org.junit.Assert.assertTrue(entrySet.isEmpty());
            org.junit.Assert.assertTrue(keySet.isEmpty());
            org.junit.Assert.assertTrue(values.isEmpty());

        }

    }

    @Test
    public void keySetClearReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            keySet.clear();
            org.junit.Assert.assertTrue(map.isEmpty());
            org.junit.Assert.assertTrue(entrySet.isEmpty());
            org.junit.Assert.assertTrue(keySet.isEmpty());
            org.junit.Assert.assertTrue(values.isEmpty());

        }
    }

    @Test
    public void valuesClearReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(3)) {
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            values.clear();
            org.junit.Assert.assertTrue(map.isEmpty());
            org.junit.Assert.assertTrue(entrySet.isEmpty());
            org.junit.Assert.assertTrue(keySet.isEmpty());
            org.junit.Assert.assertTrue(values.isEmpty());

        }

    }

    @Test
    public void clearMapViaEntryIteratorRemoves() throws IOException {
        int noOfElements = 16 * 1024;
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(noOfElements)) {

            int sum = 0;
            for (Iterator it = map.entrySet().iterator(); it.hasNext(); ) {
                it.next();
                it.remove();
                ++sum;

            }

            assertEquals(noOfElements, sum);

        }

    }

    @Test
    public void clearMapViaKeyIteratorRemoves() throws IOException {
        int noOfElements = 16 * 1024;
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(noOfElements)) {

            Set<Integer> keys = new HashSet<Integer>();
            for (int i = 1; i <= noOfElements; i++) {
                keys.add(i);
            }

            int sum = 0;
            for (Iterator it = map.keySet().iterator(); it.hasNext(); ) {
                Object key = it.next();
                keys.remove(key);
                it.remove();
                ++sum;
            }

            assertEquals(noOfElements, sum);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testRemoveWhenNextIsNotCalled() throws IOException {

        ChronicleMap<Integer, CharSequence> map = getViewTestMap(2);

        Iterator<Integer> iterator = map.keySet().iterator();
        iterator.remove();
    }

    @Test
    public void clearMapViaValueIteratorRemoves() throws IOException {
        int noOfElements = 16 * 1024;
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(noOfElements)) {

            int sum = 0;
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                it.next();
                it.remove();
                ++sum;
            }


            assertEquals(noOfElements, sum);
        }
    }

    @Test
    public void entrySetValueReflectedInMapAndOtherViews() throws IOException {
        try (ChronicleMap<Integer, CharSequence> map = getViewTestMap(0)) {

            map.put(1, "A");
            Set<Map.Entry<Integer, CharSequence>> entrySet = map.entrySet();
            Set<Integer> keySet = map.keySet();
            Collection<CharSequence> values = map.values();

            assertMap(map, new int[]{1}, new CharSequence[]{"A"});
            assertEntrySet(entrySet, new int[]{1}, new CharSequence[]{"A"});
            assertKeySet(keySet, new int[]{1});
            assertValues(values, new String[]{"A"});

            entrySet.iterator().next().setValue("B");
            assertMap(map, new int[]{1}, new CharSequence[]{"B"});
            assertEntrySet(entrySet, new int[]{1}, new CharSequence[]{"B"});
            assertEntrySet(map.entrySet(), new int[]{1}, new CharSequence[]{"B"});
            assertKeySet(keySet, new int[]{1});
            assertKeySet(map.keySet(), new int[]{1});
            assertValues(values, new String[]{"B"});
            assertValues(map.values(), new String[]{"B"});

        }

    }


    @Test
    public void testEntriesSpanningSeveralChunksPutRemoveReplace() throws IOException {
        ChronicleMapBuilder builder = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(10)
                .minSegments(1)
                .actualChunkSize(10);
        try (VanillaChronicleMap<CharSequence, ?, ?, CharSequence, ?, ?> map =
                     (VanillaChronicleMap<CharSequence, ?, ?, CharSequence, ?, ?>)
                             builder.create()) {
            String key = "k";
            String largeKey = "very large key";
            String value = "v";
            String largeValue = "vary large value";

            map.put(key, largeValue);
            assertEquals(largeValue, map.get(key));
            map.replace(key, value);
            map.checkConsistency();
            assertEquals(value, map.get(key));
            map.replace(key, largeValue);
            map.checkConsistency();
            assertEquals(largeValue, map.get(key));

            map.put(largeKey, value);
            map.checkConsistency();
            assertEquals(value, map.get(largeKey));
            map.replace(largeKey, largeValue);
            map.checkConsistency();
            assertEquals(largeValue, map.get(largeKey));
            map.remove(largeKey);
            map.checkConsistency();
            assertEquals(null, map.get(largeKey));
            map.put(largeKey, largeValue);
            map.checkConsistency();
            assertEquals(largeValue, map.get(largeKey));
            map.replace(largeKey, largeValue, value);
            map.checkConsistency();
            assertEquals(value, map.get(largeKey));

            assertEquals(value, map.remove(largeKey));
            map.checkConsistency();
            assertEquals(largeValue, map.remove(key));
            map.checkConsistency();
        }

    }


    @Test
    public void equalsTest() throws IOException {
        try (final ChronicleMap<Integer, String> map1 = ChronicleMapBuilder
                .of(Integer.class, String.class).create()) {

            map1.put(1, "one");
            map1.put(2, "two");

            try (final ChronicleMap<Integer, String> map2 = ChronicleMapBuilder
                    .of(Integer.class, String.class).create()) {

                map2.put(1, "one");
                map2.put(2, "two");

                assertEquals(map1, map2);
            }
        }


    }

    private static final class IncrementRunnable implements Runnable {

        private final ChronicleMap<CharSequence, LongValue> map;

        private final CharSequence key;

        private final int iterations;

        private final CyclicBarrier barrier;

        private IncrementRunnable(ChronicleMap<CharSequence, LongValue> map, CharSequence key, int iterations, CyclicBarrier barrier) {
            this.map = map;
            this.key = key;
            this.iterations = iterations;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                LongValue value = new LongValue$$Native();
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    map.acquireUsing(key, value);
                    value.addAtomicValue(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    @Test
    public void testPutLongValue() throws IOException {
        final ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, LongValue.class)
                .entries(1000)
                .averageKeySize("x".length());

        try (final ChronicleMap<CharSequence, LongValue> map = builder.create()) {

            LongValue value = nativeLongValue();
            try {
                map.put("x", value);
            } catch (IllegalStateException | NullPointerException e) {
                // ok
                return;
            }
            throw new AssertionError("Should throw either IllegalStateException or " +
                    "NullPointerException, but succeed");
        }

    }

    @Test
    public void testOffheapAcquireUsingLocked() throws IOException {
        ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, LongValue.class)
                .entries(1000)
                .averageKeySize("one".length());

        try (final ChronicleMap<CharSequence, LongValue> map = builder.create()) {

            LongValue value = nativeLongValue();

            // this will add the entry
            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                assertEquals(0, context.value().getValue());
                assert value == context.value();
                LongValue value1 = context.value();
                value1.addValue(1);
            }

            // check that the entry was added
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assert value == context.value();
                assertEquals(true, context.present());
                assertEquals(1, context.value().getValue());
            }

            // this will remove the entry
            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                assert value == context.value();
                context.removeEntry();
            }

            // check that the entry was removed
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assertEquals(false, context.present());
                assertEquals(null, context.value());
            }

            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                assert value == context.value();
                assertEquals(0, context.value().getValue());
            }

            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                assert value == context.value();
                context.value().addValue(1);
            }

            // check that the entry was removed
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assert value == context.value();
                assertEquals(1, context.value().getValue());
            }

        }
    }


    @Test
    public void testByteArrayKeySizeBySample() throws IOException {
        TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(8877)
                .name("serverMap");


        // this test only appear to fail when we reuse the mapFile
        for (int i = 0; i < 2; i++) {

            try (ChronicleMap server = ChronicleMapBuilder.of(byte[].class, byte[][].class)
                    .replication((byte) 1, serverConfig)
                    .constantKeySizeBySample(new byte[14])
                    .create()) {

                try (ChronicleMap<byte[], byte[][]> map2 = localClient(8877)) {


                    byte[] key = new byte[14];
                    System.arraycopy("A".getBytes(), 0, key, 0, "A".length());
                    byte[][] value = {new byte[11], new byte[11]};
                    System.arraycopy("A".getBytes(), 0, value[0], 0, "A".length());
                    System.arraycopy("A".getBytes(), 0, value[1], 0, "A".length());


                    map2.put(key, value);
                    Assert.assertNotNull(map2.get(key));
                }
            }

        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireUsingLockedWithString() throws IOException {

        ChronicleMapBuilder<CharSequence, String> builder = ChronicleMapBuilder
                .of(CharSequence.class, String.class)
                .entries(1000)
                .defaultValue("");

        try (final ChronicleMap<CharSequence, String> map = builder.create()) {
            // this will add the entry
            try (WriteContext<?, String> context = map.acquireUsingLocked("one", "")) {
                // do nothing
            }
        }
    }

    @Test
    public void testOnheapAcquireUsingLockedStringBuilder() throws IOException {

        try (final ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .averageKeySize("one".length())
                .averageValueSize("Hello World".length())
                .defaultValue("")
                .create()) {
            StringBuilder value = new StringBuilder();

            try (WriteContext<?, CharSequence> context = map.acquireUsingLocked("one", value)) {
                value.append("Hello World");
            }

            assertEquals("Hello World", value.toString());
        }
    }

    @Test
    public void testOnheapAcquireUsingLocked() throws IOException {
        File tmpFile = File.createTempFile("testAcquireUsingLocked", ".deleteme");
        tmpFile.deleteOnExit();
        try (final ChronicleMap<CharSequence, LongValue> map = ChronicleMapBuilder
                .of(CharSequence.class, LongValue.class)
                .entries(1000)
                .averageKeySize("one".length()).createPersistedTo(tmpFile)) {

            LongValue value = DataValueClasses.newDirectReference(LongValue.class);

            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assertEquals(false, context.present());
                assertEquals(null, context.value());
            }

            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                value.setValue(10);
            }

            // this will add the entry
            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                LongValue value1 = context.value();
                value1.addValue(1);
            }

            // check that the entry was added
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assertEquals(true, context.present());
                assertEquals(11, context.value().getValue());
            }

            // this will remove the entry
            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                context.removeEntry();
            }

            // check that the entry was removed
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                assertEquals(false, context.present());
                assertEquals(null, context.value());
            }

            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                assertEquals(0, context.value().getValue());
            }

            value.setValue(1);

            try (ReadContext<?, LongValue> lockedEntry = map.getUsingLocked("one", value)) {
                assertEquals(1, lockedEntry.value().getValue());
            }

            try (WriteContext<?, LongValue> context = map.acquireUsingLocked("one", value)) {
                LongValue value1 = context.value();
                value1.addValue(1);
            }

            // check that the entry was removed
            try (ReadContext<?, LongValue> context = map.getUsingLocked("one", value)) {
                LongValue value1 = context.value();
                assertEquals(2, value1.getValue());
            }
        }
        tmpFile.delete();
    }

}


