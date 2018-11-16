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

import com.google.common.primitives.Chars;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.values.*;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.MapMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import net.openhft.chronicle.hash.serialization.impl.*;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Range;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;

import static java.lang.Math.max;
import static java.util.Arrays.asList;
import static net.openhft.chronicle.core.Maths.divideRoundUp;
import static net.openhft.chronicle.map.fromdocs.OpenJDKAndHashMapExamplesTest.parseYYYYMMDD;
import static org.junit.Assert.*;

enum ToString implements SerializableFunction<Object, String> {
    INSTANCE;

    @Override
    public String apply(Object o) {
        return String.valueOf(o);
    }

}

interface IData extends BytesMarshallable {
    String getText();

    void setText(@MaxUtf8Length(64) String text);

    int getNumber();

    void setNumber(int number);

    class Data implements IData, BytesMarshallable {
        static final long MAGIC = 0x8081828384858687L;
        static final long MAGIC2 = 0xa0a1a2a3a4a5a6a7L;

        String text;
        int number;

        @Override
        public String getText() {
            return text;
        }

        @Override
        public void setText(String text) {
            this.text = text;
        }

        @Override
        public int getNumber() {
            return number;
        }

        @Override
        public void setNumber(int number) {
            this.number = number;
        }

        @Override
        public void readMarshallable(@NotNull BytesIn in) throws IllegalStateException {
            long magic = in.readLong();
            if (magic != MAGIC)
                throw new AssertionError("Start " + Long.toHexString(magic));
            text = in.readUTFΔ();
            number = in.readInt();
            long magic2 = in.readLong();
            if (magic2 != MAGIC2)
                throw new AssertionError("End " + Long.toHexString(magic2));
        }

        @Override
        public void writeMarshallable(@NotNull BytesOut out) {
            out.writeLong(MAGIC);
            out.writeUTFΔ(text);
            out.writeInt(number);
            out.writeLong(MAGIC2);
        }
    }
}

interface IBean {
    long getLong();

    void setLong(long num);

    double getDouble();

    void setDouble(double d);

    int getInt();

    void setInt(int i);

    @Array(length = 7)
    void setInnerAt(int index, Inner inner);

    Inner getInnerAt(int index);

    /* nested interface - empowering an Off-Heap hierarchical “TIER of prices”
    as array[ ] value */
    interface Inner {

        String getMessage();

        void setMessage(@MaxUtf8Length(20) String px);

    }
}

/**
 * This test enumerates common use cases for keys and values.
 */
@RunWith(value = Parameterized.class)
public class CHMUseCasesTest {

    static volatile int i = 0;
    private final TypeOfMap typeOfMap;

    Collection<Closeable> closeables = new ArrayList<Closeable>();
    ChronicleMap map1;
    ChronicleMap map2;

    public CHMUseCasesTest(TypeOfMap typeOfMap) {
        this.typeOfMap = typeOfMap;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {
                        TypeOfMap.SIMPLE
                },

                {
                        TypeOfMap.SIMPLE_PERSISTED
                }
        });

    }

    private static void appendMode(ByteBuffer valueA) {
        valueA.position(valueA.limit());
        valueA.limit(valueA.capacity());
    }

    public static <K, V> Map<K, V> mapOf(K k, V v, Object... keysAndValues) {
        Map<K, V> ret = new LinkedHashMap<>();
        ret.put(k, v);
        for (int i = 0; i < keysAndValues.length - 1; i += 2) {
            Object key = keysAndValues[i];
            Object value = keysAndValues[i + 1];
            ret.put((K) key, (V) value);
        }
        return ret;
    }

    @After
    public void after() {
        for (Closeable c : closeables) {

            try {
                c.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        closeables.clear();
        map2 = null;
        map1 = null;
    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     */
    private void waitTillEqual(final int timeOutMs) {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            Jvm.pause(1);
        }
    }

    private void mapChecks() {
        if (typeOfMap == TypeOfMap.SIMPLE)
            checkJsonSerialization();
    }

    private void assertArrayValueEquals(ChronicleMap map1, ChronicleMap map2) {

        assertEquals(map1.size(), map2.size());

        for (Object key : map1.keySet()) {

            if (map1.valueClass() == byte[].class)
                Assert.assertArrayEquals((byte[]) map1.get(key), (byte[]) map2.get(key));

            else if (map1.valueClass() == char[].class)
                Assert.assertArrayEquals((char[]) map1.get(key), (char[]) map2.get(key));
            else if (map1.valueClass() == byte[][].class) {
                byte[][] o1 = (byte[][]) map1.get(key);
                byte[][] o2 = (byte[][]) map2.get(key);

                Assert.assertEquals(o1.length, o2.length);
                for (int i = 0; i < o1.length; i++) {
                    Assert.assertArrayEquals(o1[i], o2[i]);
                }

            } else throw new IllegalStateException("unsupported type");

        }
    }

    private void checkJsonSerialization() {
        File file = null;
        try {
            file = File.createTempFile("chronicle-map-", ".json");
        } catch (IOException e) {
            e.printStackTrace();
        }

        file.deleteOnExit();
        try {

            map1.getAll(file);

            VanillaChronicleMap vanillaMap = (VanillaChronicleMap) map1;
            ChronicleMapBuilder builder = ChronicleMap
                    .of(map1.keyClass(), map1.valueClass())
                    .entriesPerSegment(
                            max(divideRoundUp(map1.size(), vanillaMap.actualSegments), 1))
                    .actualSegments(vanillaMap.actualSegments)
                    .actualChunksPerSegmentTier(vanillaMap.actualChunksPerSegmentTier);
            if (!vanillaMap.constantlySizedEntry) {
                builder.actualChunkSize((int) vanillaMap.chunkSize);
                builder.worstAlignment(vanillaMap.worstAlignment);
            }

            try (ChronicleMap actual = builder.create()) {
                actual.putAll(file);

                if (map1.valueClass() == char[].class ||
                        map1.valueClass() == byte[].class ||
                        map1.valueClass() == byte[][].class) {
                    assertArrayValueEquals(map1, actual);
                } else {
                    Assert.assertEquals(map1, actual);
                }
            }

        } catch (IOException e) {
            Assert.fail();
        } finally {
            file.delete();
        }
    }

    private <X, Y> ChronicleMap<X, Y> newInstance(ChronicleMapBuilder<X, Y> builder) throws
            IOException {
        if (!builder.constantlySizedKeys() && builder.averageKey == null)
            builder.averageKeySize(20);
        if (!builder.constantlySizedValues() && builder.averageValue == null)
            builder.averageValueSize(20);
        switch (typeOfMap) {

            case SIMPLE:
                map2 = null;
                map1 = builder.create();
                closeables.add(map1);
                return map1;

            case SIMPLE_PERSISTED:
                File file0 = null;
                try {
                    file0 = File.createTempFile("chronicle-map-", ".map");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                file0.deleteOnExit();
                final File file = file0;
                map1 = builder.createPersistedTo(file);
                closeables.add(map1);
                closeables.add(() -> file.delete());

                return map1;

            default:
                throw new IllegalStateException();
        }
    }

    @Test
    public void testArrayOfString() throws IOException {

        ChronicleMapBuilder<CharSequence, I1> builder = ChronicleMapBuilder
                .of(CharSequence.class, I1.class)
                .entries(10);

        try (ChronicleMap<CharSequence, I1> map = newInstance(builder)) {

            {
                final I1 i1 = Values.newHeapInstance(I1.class);
                i1.setStrAt(1, "Hello");
                i1.setStrAt(2, "World");
                map.put("Key1", i1);
            }

            {
                final I1 i1 = Values.newHeapInstance(I1.class);
                i1.setStrAt(1, "Hello2");
                i1.setStrAt(2, "World2");
                map.put("Key2", i1);
            }

            {
                final I1 key = map.get("Key1");

                assertEquals("Hello", key.getStrAt(1));
                assertEquals("World", key.getStrAt(2));
            }

            {
                final I1 key = map.get("Key2");

                assertEquals("Hello2", key.getStrAt(1));
                assertEquals("World2", key.getStrAt(2));
            }

            // todo not currently supported for arrays
            // mapChecks();
        }
    }

    @Test
    public void testCharArrayValue() throws IOException {

        int valueSize = 10;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class)
                .averageValue(expected)
                .entries(1);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);

            assertEquals(Chars.asList(expected), Chars.asList(map.get("Key")));
            mapChecks();
        }
    }

    @Test
    public void testByteArrayArrayValue()
            throws IOException {

        ChronicleMapBuilder<byte[], byte[][]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[][].class)
                .entries(1)
                .averageKey("Key".getBytes())
                .averageValue(new byte[][]{"value1".getBytes(), "value2".getBytes()});

        try (ChronicleMap<byte[], byte[][]> map = newInstance(builder)) {
            byte[] bytes1 = "value1".getBytes();
            byte[] bytes2 = "value2".getBytes();
            byte[][] value = {bytes1, bytes2};
            map.put("Key".getBytes(), value);

            assertEquals(value, map.get("Key".getBytes()));
            mapChecks();
        }
    }

    @Test
    public void bondExample() throws IOException {

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(String.class, BondVOInterface.class)
                .entries(1)
                .averageKeySize(10);

        try (ChronicleMap<String, BondVOInterface> chm = newInstance(builder)) {
            BondVOInterface bondVO = Values.newNativeReference(BondVOInterface.class);
            try (net.openhft.chronicle.core.io.Closeable c =
                         chm.acquireContext("369604103", bondVO)) {
                bondVO.setIssueDate(parseYYYYMMDD("20130915"));
                bondVO.setMaturityDate(parseYYYYMMDD("20140915"));
                bondVO.setCoupon(5.0 / 100); // 5.0%

                BondVOInterface.MarketPx mpx930 = bondVO.getMarketPxIntraDayHistoryAt(0);
                mpx930.setAskPx(109.2);
                mpx930.setBidPx(106.9);

                BondVOInterface.MarketPx mpx1030 = bondVO.getMarketPxIntraDayHistoryAt(1);
                mpx1030.setAskPx(109.7);
                mpx1030.setBidPx(107.6);
            }
        }

    }

    @Test
    public void testLargeCharSequenceValueWriteOnly() throws IOException {

        int valueSize = 1000000;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class).entries(1)
                .constantValueSizeBySample(expected);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            mapChecks();
        }
    }

    @Test
    public void testEntrySpanningSeveralChunks()
            throws IOException {

        int salefactor = 100;
        int valueSize = 10 * salefactor;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class)
                .entries(100)
                .averageKeySize(10)
                .averageValueSize(10);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            mapChecks();
        }
    }

    @Test
    public void testKeyValueSizeBySample() throws
            IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .averageKeySize("Key".length())
                .averageValueSize("Value".length())
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Key", "Value");
            mapChecks();
        }
    }

    @Test
    public void testLargeCharSequenceValue()
            throws IOException {

        int valueSize = 5_000_000;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class).entries(1)
                .constantValueSizeBySample(expected);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            Assert.assertArrayEquals(expected, map.get("Key"));
        }
    }

    @Test
    public void testStringStringMap() throws
            IOException {

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .entries(1);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            assertEquals("World", map.get("Hello"));

            assertEquals("New World", map.getMapped("Hello", new PrefixStringFunction("New ")));
            assertEquals(null, map.getMapped("No key", new PrefixStringFunction("New ")));
            mapChecks();
        }
    }

    @Test
    public void testStringStringMapMutableValue() throws IOException {

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .entries(1);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.computeIfPresent("Hello", new StringPrefixUnaryOperator("New "));
            mapChecks();
        }
    }

    @Test
    public void testCharSequenceMixingKeyTypes() throws IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            map.put("Hello", "World");
            map.put(new StringBuilder("Hello"), "World2");

            Assert.assertEquals("World2", map.get("Hello").toString());
            mapChecks();
        }
    }

    @Test
    public void testCharSequenceMixingValueTypes() throws IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(2);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.put("Hello2", new StringBuilder("World2"));

            Assert.assertEquals("World2", map.get("Hello2").toString());
            Assert.assertEquals("World", map.get("Hello").toString());
            mapChecks();
        }
    }

    /**
     * CharSequence is more efficient when object creation is avoided.
     * * The key can only be on heap and variable length serialised.
     */
    @Test
    public void testCharSequenceCharSequenceMap()
            throws IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(10);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Hello", "World");
            StringBuilder key = new StringBuilder();
            key.append("key-").append(1);

            StringBuilder value = new StringBuilder();
            value.append("value-").append(1);
            map.put(key, value);
            assertEquals("value-1", map.get("key-1").toString());

            assertEquals(value, map.getUsing(key, value));
            assertEquals("value-1", value.toString());
            map.remove("key-1");
            assertNull(map.getUsing(key, value));

            assertEquals("New World", map.getMapped("Hello", s -> "New " + s));
            assertEquals(null, map.getMapped("No key",
                    (SerializableFunction<CharSequence, CharSequence>) s -> "New " + s));

            assertEquals("New World !!", map.computeIfPresent("Hello", (k, s) -> {
                ((StringBuilder) s).append(" !!");
                return "New " + s;
            }).toString());

            assertEquals("New World !!", map.get("Hello").toString());

            assertEquals("New !!", map.compute("no-key", (k, s) -> {
                assertNull(s);
                return "New !!";
            }).toString());

            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithCharSequence() throws IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            CharSequence using = new StringBuilder();

            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", using)) {
                assertTrue(using instanceof StringBuilder);
                ((StringBuilder) using).append("Hello World");
            }

            assertEquals("Hello World", map.get("1").toString());
            mapChecks();
        }
    }

    @Test
    public void testGetUsingWithIntValueNoValue() throws IOException {

        ChronicleMapBuilder<CharSequence, IntValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, IntValue.class)
                .entries(1);

        try (ChronicleMap<CharSequence, IntValue> map = newInstance(builder)) {

            try (ExternalMapQueryContext<CharSequence, IntValue, ?> c = map.queryContext("1")) {
                assertNull(c.entry());
            }

            assertEquals(null, map.get("1"));
            mapChecks();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireUsingImmutableUsing() throws IOException {

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {

            IntValue using = Values.newHeapInstance(IntValue.class);
            using.setValue(1);

            try (Closeable c = map.acquireContext(using, "")) {
                assertTrue(using instanceof IntValue);
                using.setValue(1);
            }

            assertEquals(null, map.get("1"));
            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithIntValueKeyStringBuilderValue() throws IOException {

        ChronicleMapBuilder<IntValue, StringBuilder> builder = ChronicleMapBuilder
                .of(IntValue.class, StringBuilder.class)
                .entries(1);

        try (ChronicleMap<IntValue, StringBuilder> map = newInstance(builder)) {

            IntValue key = Values.newHeapInstance(IntValue.class);
            key.setValue(1);

            StringBuilder using = new StringBuilder();

            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext(key, using)) {
                using.append("Hello");
            }

            assertEquals("Hello", map.get(key).toString());
            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithIntValueKey() throws IOException {

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .entries(3);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {

            IntValue key = Values.newHeapInstance(IntValue.class);
            key.setValue(1);

            CharSequence using = new StringBuilder();

            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext(key, using)) {
                key.setValue(3);
                ((StringBuilder) using).append("Hello");
            }

            key.setValue(2);
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext(key, using)) {
                ((StringBuilder) using).append("World");
            }

            key.setValue(1);
            assertEquals("Hello", map.get(key).toString());
            mapChecks();
        }
    }

    /**
     * StringValue represents any bean which contains a String Value
     */
    @Test
    public void testStringValueStringValueMap() throws IOException {

        ChronicleMapBuilder<StringValue, StringValue> builder = ChronicleMapBuilder
                .of(StringValue.class, StringValue.class)
                .entries(10);

        try (ChronicleMap<StringValue, StringValue> map = newInstance(builder)) {
            StringValue key1 = Values.newHeapInstance(StringValue.class);
            StringValue key2 = Values.newHeapInstance(StringValue.class);
            StringValue value1 = Values.newHeapInstance(StringValue.class);
            StringValue value2 = Values.newHeapInstance(StringValue.class);

            key1.setValue(new StringBuilder("1"));
            value1.setValue("11");
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue("2");
            value2.setValue(new StringBuilder("22"));
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            mapChecks();

            StringBuilder sb = new StringBuilder();
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                StringValue v = entry.value().get();
                assertEquals("11", v.getValue().toString());
                v.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                StringValue v = entry.value().get();
                assertEquals("22", v.getValue().toString());
                v.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                StringValue v = entry.value().get();
                assertEquals("11", v.getValue().toString());
                v.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                StringValue v = entry.value().get();
                assertEquals("22", v.getValue().toString());
                v.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }

            key1.setValue("3");
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }

            key2.setValue("4");
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals("", value1.getValue().toString());
                value1.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value1.setValue(sb);
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals("123", value2.getValue().toString());
                value2.setValue(value2.getValue().toString() + '4');
                assertEquals("1234", value2.getValue().toString());
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals("1234", entry.value().get().getValue().toString());
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals("", value2.getValue().toString());
                value2.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value2.setValue(sb);
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals("123", value1.getValue().toString());
                value1.setValue(value1.getValue().toString() + '4');
                assertEquals("1234", value1.getValue().toString());
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals("1234", entry.value().get().getValue().toString());
            }

            mapChecks();
        }
    }

    @Test
    public void testIntegerIntegerMap()
            throws IOException {

        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(10);

        try (ChronicleMap<Integer, Integer> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            Integer key1;
            Integer key2;
            Integer value1;
            Integer value2;

            key1 = 1;
            value1 = 11;
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2 = 2;
            value2 = 22;
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            assertEquals((Integer) 11, map.get(key1));
            assertEquals((Integer) 22, map.get(key2));
            assertEquals(null, map.get(3));
            assertEquals(null, map.get(4));

            mapChecks();

            assertEquals((Integer) 110, map.getMapped(1, new SerializableFunction<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            mapChecks();

            assertEquals(null, map.getMapped(-1, new SerializableFunction<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            mapChecks();

            try {
                map.computeIfPresent(1, (k, s) -> s + 1);
            } catch (Exception todoMoreSpecificException) {
            }
            mapChecks();

        }
    }

    @Test
    public void testLongLongMap() throws IOException {

        ChronicleMapBuilder<Long, Long> builder = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .entries(10);

        try (ChronicleMap<Long, Long> map = newInstance(builder)) {
//            assertEquals(16, entrySize(map));
//            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            map.put(1L, 11L);
            assertEquals((Long) 11L, map.get(1L));

            map.put(2L, 22L);
            assertEquals((Long) 22L, map.get(2L));

            assertEquals(null, map.get(3L));
            assertEquals(null, map.get(4L));

            mapChecks();

            assertEquals((Long) 110L, map.getMapped(1L, new SerializableFunction<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.getMapped(-1L, (SerializableFunction<Long, Long>) s -> 10 * s));

            mapChecks();

            try {
                map.computeIfPresent(1L, (k, s) -> s + 1);
            } catch (Exception todoMoreSpecificException) {
            }

            mapChecks();
        }
    }

    @Test
    public void testDoubleDoubleMap() throws IOException {

        ChronicleMapBuilder<Double, Double> builder = ChronicleMapBuilder
                .of(Double.class, Double.class)
                .entries(10);

        try (ChronicleMap<Double, Double> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            map.put(1.0, 11.0);
            assertEquals((Double) 11.0, map.get(1.0));

            map.put(2.0, 22.0);
            assertEquals((Double) 22.0, map.get(2.0));

            assertEquals(null, map.get(3.0));
            assertEquals(null, map.get(4.0));

            assertEquals((Double) 110.0, map.getMapped(1.0, new SerializableFunction<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.getMapped(-1.0, (SerializableFunction<Double, Double>) s -> 10 * s));

            try {
                map.computeIfPresent(1.0, (k, s) -> s + 1);

            } catch (Exception todoMoreSpecificException) {
            }
        }
    }

    @Test
    public void testByteArrayByteArrayMap()
            throws IOException {

        ChronicleMapBuilder<byte[], byte[]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[].class).averageKeySize(4).averageValueSize(4)
                .entries(1000);

        try (ChronicleMap<byte[], byte[]> map = newInstance(builder)) {
            byte[] key1 = {1, 1, 1, 1};
            byte[] key2 = {2, 2, 2, 2};
            byte[] value1 = {11, 11, 11, 11};
            byte[] value2 = {22, 22, 22, 22};
            assertNull(map.put(key1, value1));
            assertTrue(Arrays.equals(value1, map.put(key1, value2)));
            assertTrue(Arrays.equals(value2, map.get(key1)));
            assertNull(map.get(key2));

            map.put(key1, value1);

            assertTrue(Arrays.equals(new byte[]{11, 11},
                    map.getMapped(key1, new SerializableFunction<byte[], byte[]>() {
                        @Override
                        public byte[] apply(byte[] s) {
                            return Arrays.copyOf(s, 2);
                        }
                    })));
            assertEquals(null, map.getMapped(key2, new SerializableFunction<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            }));

            assertTrue(Arrays.equals(new byte[]{12, 10},
                    map.computeIfPresent(key1, (k, s) -> {
                        s[0]++;
                        s[1]--;
                        return Arrays.copyOf(s, 2);
                    })));

            byte[] a2 = map.get(key1);
            assertTrue(Arrays.equals(new byte[]{12, 10}, a2));

        }
    }

    @Test
    public void testByteBufferByteBufferDefaultKeyValueMarshaller() throws
            IOException {

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(8)
                .averageValueSize(8)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11});
            ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22});
            assertNull(map.put(key1, value1));
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2));

            map.put(key1, value1);

            mapChecks();
        }
    }

    @Test
    public void testByteBufferByteBufferMap()
            throws IOException {

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(8)
                .averageValueSize(8)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11});
            ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22});
            assertNull(map.put(key1, value1));
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2));

            final SerializableFunction<ByteBuffer, ByteBuffer> function =
                    new SerializableFunction<ByteBuffer, ByteBuffer>() {
                        @Override
                        public ByteBuffer apply(ByteBuffer s) {
                            ByteBuffer slice = s.slice();
                            slice.limit(2);
                            return slice;
                        }
                    };

            map.put(key1, value1);
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.getMapped(key1, function));
            assertEquals(null, map.getMapped(key2, function));
            mapChecks();
            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}),
                    map.computeIfPresent(key1, (k, s) -> {
                        s.put(0, (byte) (s.get(0) + 1));
                        s.put(1, (byte) (s.get(1) - 1));
                        return function.apply(s);
                    }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.get(key1));

            mapChecks();

            map.put(key1, value1);
            map.put(key2, value2);
            ByteBuffer valueA = ByteBuffer.allocateDirect(8);
            ByteBuffer valueB = ByteBuffer.allocate(8);
//            assertBBEquals(value1, valueA);
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key1)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry);
                assertBBEquals(value1, entry.value().getUsing(valueA));
            }
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key2)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry);
                assertBBEquals(value2, entry.value().getUsing(valueA));
            }

            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key1)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry);
                assertBBEquals(value1, entry.value().getUsing(valueB));
            }
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key2)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry);
                assertBBEquals(value2, entry.value().getUsing(valueB));
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, valueA)) {
                assertBBEquals(value1, valueA);
                appendMode(valueA);
                valueA.clear();
                valueA.putInt(12345);
                valueA.flip();
            }

            value1.clear();
            value1.putInt(12345);
            value1.flip();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, valueB)) {
                assertBBEquals(value1, valueB);
                appendMode(valueB);
                valueB.putShort((short) 12345);
                valueB.flip();
            }

            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key1)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry);

                ByteBuffer bb1 = ByteBuffer.allocate(8);
                bb1.put(value1);
                bb1.putShort((short) 12345);
                bb1.flip();
                assertBBEquals(bb1, entry.value().getUsing(valueA));
            }

            mapChecks();
        }
    }

    @Test
    public void testByteBufferDirectByteBufferMap()
            throws IOException {

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(5).averageValueSize(5)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            ByteBuffer value1 = ByteBuffer.allocateDirect(4);
            value1.put(new byte[]{11, 11, 11, 11});
            value1.flip();
            ByteBuffer value2 = ByteBuffer.allocateDirect(4);
            value2.put(new byte[]{22, 22, 22, 22});
            value2.flip();
            assertNull(map.put(key1, value1));
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2));
            map.put(key1, value1);
            mapChecks();
            final SerializableFunction<ByteBuffer, ByteBuffer> function =
                    s -> {
                        ByteBuffer slice = s.slice();
                        slice.limit(2);
                        return slice;
                    };
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.getMapped(key1, function));
            assertEquals(null, map.getMapped(key2, function));
            mapChecks();
            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}),
                    map.computeIfPresent(key1, (k, s) -> {
                        s.put(0, (byte) (s.get(0) + 1));
                        s.put(1, (byte) (s.get(1) - 1));
                        return function.apply(s);
                    }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.get(key1));
            mapChecks();
        }
    }

    private void assertBBEquals(ByteBuffer bb1, ByteBuffer bb2) {
        assertEquals(bb1.remaining(), bb2.remaining());
        for (int i = 0; i < bb1.remaining(); i++)
            assertEquals(bb1.get(bb1.position() + i), bb2.get(bb2.position() + i));
    }

    @Test
    public void testIntValueIntValueMap() throws IOException {

        ChronicleMapBuilder<IntValue, IntValue> builder = ChronicleMapBuilder
                .of(IntValue.class, IntValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, IntValue> map = newInstance(builder)) {
            // this may change due to alignment
//            assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            IntValue value1 = Values.newHeapInstance(IntValue.class);
            IntValue value2 = Values.newHeapInstance(IntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO review -- the previous version of this block:
            // acquiring for value1, comparing value2 -- as intended?
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }
            mapChecks();

        }
    }

    /**
     * For unsigned int -> unsigned int entries, the key can be on heap or off heap.
     */
    @Test
    public void testUnsignedIntValueUnsignedIntValueMap() throws IOException {

        ChronicleMapBuilder<UnsignedIntValue, UnsignedIntValue> builder = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class)
                .entries(10);

        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            UnsignedIntValue key1 = Values.newHeapInstance(UnsignedIntValue.class);
            UnsignedIntValue value1 = Values.newHeapInstance(UnsignedIntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key1 = Values.newHeapInstance(UnsignedIntValue.class);
            UnsignedIntValue key2 = Values.newHeapInstance(UnsignedIntValue.class);
            value1 = Values.newHeapInstance(UnsignedIntValue.class);
            UnsignedIntValue value2 = Values.newHeapInstance(UnsignedIntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO review suspicious block
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueShortValueMap() throws IOException {

        ChronicleMapBuilder<IntValue, ShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, ShortValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, ShortValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(6, entrySize(map));

            //     assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            ShortValue value1 = Values.newHeapInstance(ShortValue.class);
            ShortValue value2 = Values.newHeapInstance(ShortValue.class);

            key1.setValue(1);
            value1.setValue((short) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue((short) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO the same as above.
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue((short) 123);
                assertEquals(123, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue((short) (1230 - 123));
                assertEquals(1230, value2.getValue());
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((short) 123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((short) (1230 - 123));
                assertEquals(1230, value1.getValue());
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int -> unsigned short values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedShortValueMap() throws IOException {

        ChronicleMapBuilder<IntValue, UnsignedShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedShortValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, UnsignedShortValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            UnsignedShortValue value1 = Values.newHeapInstance(UnsignedShortValue.class);
            UnsignedShortValue value2 = Values.newHeapInstance(UnsignedShortValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO the same as above.
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueCharValueMap() throws IOException {

        ChronicleMapBuilder<IntValue, CharValue> builder = ChronicleMapBuilder
                .of(IntValue.class, CharValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, CharValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            CharValue value1 = Values.newHeapInstance(CharValue.class);
            CharValue value2 = Values.newHeapInstance(CharValue.class);

            key1.setValue(1);
            value1.setValue((char) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue((char) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO The same as above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals('\0', value1.getValue());
                value1.setValue('@');
                assertEquals('@', value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext(key1, value2)) {
                assertEquals('@', value2.getValue());
                value2.setValue('#');
                assertEquals('#', value2.getValue());
            }
            try (ExternalMapQueryContext<IntValue, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals('#', entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals('\0', value2.getValue());
                value2.setValue(';');
                assertEquals(';', value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(';', value1.getValue());
                value1.setValue('[');
                assertEquals('[', value1.getValue());
            }
            try (ExternalMapQueryContext<IntValue, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, CharValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals('[', entry.value().get().getValue());
            }
        }
    }

    /**
     * For int-> byte entries, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedByteMap() throws IOException {

        ChronicleMapBuilder<IntValue, UnsignedByteValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedByteValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, UnsignedByteValue> map = newInstance(builder)) {

            // TODO should be 5, but shorter fields based on range doesn't seem to be implemented
            // on data value generation level yet
            //assertEquals(8, entrySize(map)); this may change due to alignmented
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            UnsignedByteValue value1 = Values.newHeapInstance(UnsignedByteValue.class);
            UnsignedByteValue value2 = Values.newHeapInstance(UnsignedByteValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO the same as above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(234);
                assertEquals(234, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(234, value2.getValue());
                value2.addValue(-100);
                assertEquals(134, value2.getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(134, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((byte) 123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((byte) -111);
                assertEquals(12, value1.getValue());
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(12, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueBooleanValueMap() throws IOException {

        ChronicleMapBuilder<IntValue, BooleanValue> builder = ChronicleMapBuilder
                .of(IntValue.class, BooleanValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, BooleanValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            IntValue key1 = Values.newHeapInstance(IntValue.class);
            IntValue key2 = Values.newHeapInstance(IntValue.class);
            BooleanValue value1 = Values.newHeapInstance(BooleanValue.class);
            BooleanValue value2 = Values.newHeapInstance(BooleanValue.class);

            key1.setValue(1);
            value1.setValue(true);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(false);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(true, entry.value().get().getValue());
            }
            // TODO the same as above. copy paste, copy paste, copy-paste...
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(false, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(false, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(true, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(false, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(false, value1.getValue());
                value1.setValue(true);
                assertEquals(true, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(true, value2.getValue());
                value2.setValue(false);
                assertEquals(false, value2.getValue());
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(false, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(false, value2.getValue());
                value2.setValue(true);
                assertEquals(true, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(true, value1.getValue());
                value1.setValue(false);
                assertEquals(false, value1.getValue());
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(false, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For float values, the key can be on heap or off heap.
     */
    @Test
    public void testFloatValueFloatValueMap() throws IOException {

        ChronicleMapBuilder<FloatValue, FloatValue> builder = ChronicleMapBuilder
                .of(FloatValue.class, FloatValue.class)
                .entries(10);

        try (ChronicleMap<FloatValue, FloatValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            FloatValue key1 = Values.newHeapInstance(FloatValue.class);
            FloatValue key2 = Values.newHeapInstance(FloatValue.class);
            FloatValue value1 = Values.newHeapInstance(FloatValue.class);
            FloatValue value2 = Values.newHeapInstance(FloatValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue(), 0);
            }
            // TODO see above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue(), 0);
//            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue(), 0);
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue(), 0);
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue(), 0);
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue(), 0);
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue(), 0);
            }
            mapChecks();
        }
    }

    /**
     * For double values, the key can be on heap or off heap.
     */
    @Test
    public void testDoubleValueDoubleValueMap() throws IOException {

        ChronicleMapBuilder<DoubleValue, DoubleValue> builder = ChronicleMapBuilder
                .of(DoubleValue.class, DoubleValue.class)
                .entries(10);

        try (ChronicleMap<DoubleValue, DoubleValue> map = newInstance(builder)) {

            // this may change due to alignment
            //assertEquals(16, entrySize(map));

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            DoubleValue key1 = Values.newHeapInstance(DoubleValue.class);
            DoubleValue key2 = Values.newHeapInstance(DoubleValue.class);
            DoubleValue value1 = Values.newHeapInstance(DoubleValue.class);
            DoubleValue value2 = Values.newHeapInstance(DoubleValue.class);

            key1.setValue(1);
            value1.setValue(11);
            assertEquals(null, map.get(key1));

            map.put(key1, value1);
            DoubleValue v2 = map.get(key1);
            assertEquals(value1, v2);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue(), 0);
            }
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue(), 0);
//            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue(), 0);
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue(), 0);
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue(), 0);
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue(), 0);
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue(), 0);
            }
            mapChecks();
        }
    }

    /**
     * For long values, the key can be on heap or off heap.
     */
    @Test
    public void testLongValueLongValueMap() throws IOException {

        ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMapBuilder
                .of(LongValue.class, LongValue.class)
                .entries(10);

        try (ChronicleMap<LongValue, LongValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(16, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            LongValue key1 = Values.newHeapInstance(LongValue.class);
            LongValue key2 = Values.newHeapInstance(LongValue.class);
            LongValue value1 = Values.newHeapInstance(LongValue.class);
            LongValue value2 = Values.newHeapInstance(LongValue.class);

            key1.setValue(1);
            value1.setValue(11);
            assertEquals(null, map.get(key1));
            map.put(key1, value1);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            // TODO see above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(11, entry.value().get().getValue());
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(22, entry.value().get().getValue());
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry());
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry);
                assertEquals(1230, entry.value().get().getValue());
            }
            mapChecks();
        }
    }

    @Test
    public void testListValue() throws IOException {

        ChronicleMapBuilder<String, List<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<List<String>>) (Class) List.class)
                .entries(2)
                .valueMarshaller(ListMarshaller.of(
                        new StringBytesReader(), CharSequenceBytesWriter.INSTANCE));

        try (ChronicleMap<String, List<String>> map = newInstance(builder)) {
            map.put("1", Collections.emptyList());
            map.put("2", asList("two-A"));

            List<String> list1 = new ArrayList<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", list1)) {
                list1.add("one");
                assertEquals(asList("one"), list1);
            }
            List<String> list2 = new ArrayList<>();
            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("1")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(asList("one"), entry.value().getUsing(list2));
            }

            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("2")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry);
                entry.value().getUsing(list2);
                list2.add("two-B");     // this is not written as it only a read context
                assertEquals(asList("two-A", "two-B"), list2);
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", list1)) {
                list1.add("two-C");
                assertEquals(asList("two-A", "two-C"), list1);
            }

            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("2")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(asList("two-A", "two-C"), entry.value().getUsing(list2));
            }
        }
    }

    @Test
    public void testSetValue() throws IOException {
        ChronicleMapBuilder<String, Set<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Set<String>>) (Class) Set.class)
                .entries(10)
                .valueMarshaller(SetMarshaller.of(
                        new StringBytesReader(), CharSequenceBytesWriter.INSTANCE));

        try (ChronicleMap<String, Set<String>> map = newInstance(builder)) {
            map.put("1", Collections.emptySet());
            map.put("2", new LinkedHashSet<>(asList("one")));

            Set<String> list1 = new LinkedHashSet<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", list1)) {
                list1.add("two");
                assertEquals(new LinkedHashSet<>(asList("two")), list1);
            }
            Set<String> list2 = new LinkedHashSet<>();
            try (ExternalMapQueryContext<String, Set<String>, ?> c = map.queryContext("1")) {
                MapEntry<String, Set<String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(new LinkedHashSet<>(asList("two")), entry.value().getUsing(list2));
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", list1)) {
                list1.add("three");
                assertEquals(new LinkedHashSet<String>(asList("one", "three")), list1);
            }
            try (ExternalMapQueryContext<String, Set<String>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Set<String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(new LinkedHashSet<>(asList("one", "three")),
                        entry.value().getUsing(list2));
            }

            for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                entry.getKey();
                entry.getValue();
            }

            mapChecks();
        }
    }

    @Test
    public void testMapStringStringValue() throws IOException {

        MapMarshaller<String, String> valueMarshaller = new MapMarshaller<>(new StringBytesReader(), CharSequenceBytesWriter.INSTANCE,
                new StringBytesReader(), CharSequenceBytesWriter.INSTANCE);
        ChronicleMapBuilder<String, Map<String, String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, String>>) (Class) Map.class)
                .entries(3)
                .valueMarshaller(valueMarshaller);

        try (ChronicleMap<String, Map<String, String>> map = newInstance(builder)) {
            map.put("1", Collections.emptyMap());
            map.put("2", mapOf("one", "uni"));

            Map<String, String> map1 = new LinkedHashMap<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", map1)) {
                map1.put("two", "bi");
                assertEquals(mapOf("two", "bi"), map1);
            }
            Map<String, String> map2 = new LinkedHashMap<>();
            try (ExternalMapQueryContext<String, Map<String, String>, ?> c =
                         map.queryContext("1")) {
                MapEntry<String, Map<String, String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(mapOf("two", "bi"), entry.value().getUsing(map2));
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", map1)) {
                map1.put("three", "tri");
                assertEquals(mapOf("one", "uni", "three", "tri"), map1);
            }
            try (ExternalMapQueryContext<String, Map<String, String>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Map<String, String>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(mapOf("one", "uni", "three", "tri"), entry.value().getUsing(map2));
            }
            mapChecks();
        }
    }

    @Test
    public void testMapStringIntegerValue() throws IOException {

        MapMarshaller<String, Integer> valueMarshaller = new MapMarshaller<>(
                new StringBytesReader(), CharSequenceBytesWriter.INSTANCE,
                IntegerMarshaller.INSTANCE, IntegerMarshaller.INSTANCE);
        ChronicleMapBuilder<String, Map<String, Integer>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class)
                .entries(10)
                .valueMarshaller(valueMarshaller);

        try (ChronicleMap<String, Map<String, Integer>> map = newInstance(builder)) {
            map.put("1", Collections.emptyMap());
            map.put("2", mapOf("one", 1));

            Map<String, Integer> map1 = new LinkedHashMap<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", map1)) {
                map1.put("two", 2);
                assertEquals(mapOf("two", 2), map1);
            }
            Map<String, Integer> map2 = new LinkedHashMap<>();
            try (ExternalMapQueryContext<String, Map<String, Integer>, ?> c =
                         map.queryContext("1")) {
                MapEntry<String, Map<String, Integer>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(mapOf("two", 2), entry.value().getUsing(map2));
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", map1)) {
                map1.put("three", 3);
                assertEquals(mapOf("one", 1, "three", 3), map1);
            }
            try (ExternalMapQueryContext<String, Map<String, Integer>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Map<String, Integer>> entry = c.entry();
                assertNotNull(entry);
                assertEquals(mapOf("one", 1, "three", 3), entry.value().getUsing(map2));
            }
            mapChecks();
        }
    }

    @Test
    public void testMapStringIntegerValueWithoutListMarshallers() throws IOException {
        ChronicleMapBuilder<String, Map<String, Integer>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class)
                .averageKey("2")
                .averageValue(mapOf("two", 2))
                .entries(2);
        try (ChronicleMap<String, Map<String, Integer>> map = newInstance(builder)) {
            map.put("1", Collections.emptyMap());
            map.put("2", mapOf("two", 2));

            assertEquals(mapOf("two", 2), map.get("2"));
            mapChecks();
        }
    }

    @Test
    public void testGeneratedDataValue() throws IOException {

        ChronicleMapBuilder<String, IBean> builder = ChronicleMapBuilder
                .of(String.class, IBean.class).averageKeySize(5).entries(1000);
        try (ChronicleMap<String, IBean> map = newInstance(builder)) {

            IBean iBean = Values.newNativeReference(IBean.class);
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", iBean)) {
                iBean.setDouble(1.2);
                iBean.setLong(2);
                iBean.setInt(4);
                IBean.Inner innerAt = iBean.getInnerAt(1);
                innerAt.setMessage("Hello world");
            }

            assertEquals(2, map.get("1").getLong());
            assertEquals("Hello world", map.get("1").getInnerAt(1).getMessage());
            mapChecks();
        }
    }

    @Test
    public void testBytesMarshallable() throws IOException {
        ChronicleMapBuilder<IData, IData> builder = ChronicleMapBuilder
                .of(IData.class, IData.class)
                .entries(1000);
        try (ChronicleMap<IData, IData> map = newInstance(builder)) {
            for (int i = 0; i < 100; i++) {
                IData key = Values.newHeapInstance(IData.class);
                IData value = Values.newHeapInstance(IData.class);
                key.setText("key-" + i);
                key.setNumber(i);
                value.setNumber(i);
                value.setText("value-" + i);
                map.put(key, value);
                // check the map is still valid.
                map.entrySet().toString();
            }
        }
    }

    @Test
    public void testBytesMarshallable2() throws IOException {
        ChronicleMapBuilder<IData.Data, IData.Data> builder = ChronicleMapBuilder
                .of(IData.Data.class, IData.Data.class)
                .keyReaderAndDataAccess(new DataReader(), new DataDataAccess())
                .valueReaderAndDataAccess(new DataReader(), new DataDataAccess())
                .actualChunkSize(64)
                .entries(1000);
        try (ChronicleMap<IData.Data, IData.Data> map = newInstance(builder)) {
            for (int i = 0; i < 100; i++) {
                IData.Data key = new IData.Data();
                IData.Data value = new IData.Data();
                key.setText("key-" + i);
                key.setNumber(i);
                value.setNumber(i);
                value.setText("value-" + i);
                map.put(key, value);
                // check the map is still valid.
                map.entrySet().toString();
            }
        }
    }

    enum TypeOfMap {SIMPLE, SIMPLE_PERSISTED}

    interface I1 {
        @Array(length = 10)
        String getStrAt(int i);

        void setStrAt(int i, @MaxUtf8Length(10) String str);
    }

    interface StringValue {
        CharSequence getValue();

        void setValue(@net.openhft.chronicle.values.NotNull @MaxUtf8Length(64) CharSequence value);

        void getUsingValue(StringBuilder using);
    }

    interface UnsignedIntValue {
        long getValue();

        void setValue(@Range(min = 0, max = (1L << 32) - 1) long value);

        long addValue(long addition);
    }

    interface UnsignedShortValue {
        int getValue();

        void setValue(@Range(min = 0, max = Character.MAX_VALUE) int value);

        int addValue(int addition);
    }

    interface UnsignedByteValue {
        int getValue();

        void setValue(@Range(min = 0, max = 255) int value);

        int addValue(int addition);
    }

    static class PrefixStringFunction implements SerializableFunction<String, String> {
        private final String prefix;

        public PrefixStringFunction(@NotNull String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String apply(String s) {
            return prefix + s;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof PrefixStringFunction &&
                    prefix.equals(((PrefixStringFunction) obj).prefix);
        }

        @Override
        public String toString() {
            return prefix;
        }
    }

    private static class StringPrefixUnaryOperator
            implements BiFunction<String, String, String>, Serializable {

        private String prefix;

        StringPrefixUnaryOperator(final String prefix1) {
            prefix = prefix1;
        }

        @Override
        public String apply(String k, String v) {
            return prefix + v;
        }
    }

    private static class DataDataAccess extends BytesMarshallableDataAccess<IData.Data> {
        public DataDataAccess() {
            super(IData.Data.class);
        }

        @Override
        protected IData.Data createInstance() {
            return new IData.Data();
        }

        @Override
        public DataAccess<IData.Data> copy() {
            return new DataDataAccess();
        }
    }

    private static class DataReader extends BytesMarshallableReader<IData.Data> {
        public DataReader() {
            super(IData.Data.class);
        }

        @Override
        protected IData.Data createInstance() {
            return new IData.Data();
        }
    }
}
