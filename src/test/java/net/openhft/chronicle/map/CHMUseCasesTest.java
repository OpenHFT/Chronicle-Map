/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.google.common.primitives.Chars;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.BiFunction;

import static java.lang.Math.max;
import static java.util.Arrays.asList;
import static net.openhft.chronicle.core.Maths.divideRoundUp;
import static net.openhft.chronicle.map.fromdocs.OpenJDKAndHashMapExamplesTest.parseYYYYMMDD;
import static org.junit.jupiter.api.Assertions.*;

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
        public void readMarshallable(@NotNull BytesIn<?> in) throws IllegalStateException {
            long magic = in.readLong();
            if (magic != MAGIC)
                throw new AssertionError("Start " + Long.toHexString(magic));
            text = in.readUtf8();
            number = in.readInt();
            long magic2 = in.readLong();
            if (magic2 != MAGIC2)
                throw new AssertionError("End " + Long.toHexString(magic2));
        }

        @Override
        public void writeMarshallable(@NotNull BytesOut<?> out) {
            out.writeLong(MAGIC);
            out.writeUtf8(text);
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
@SuppressWarnings({"rawtypes", "unchecked", "try", "serial"})
public class CHMUseCasesTest {

    private TypeOfMap typeOfMap;
    private final Collection<Closeable> closeables = new ArrayList<>();
    private ChronicleMap<?, ?> map1;

    public static Collection<TypeOfMap> typeOfMaps() {
        return asList(TypeOfMap.SIMPLE, TypeOfMap.SIMPLE_PERSISTED);
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

    @AfterEach
    public void after() {
        for (Closeable c : closeables) {
            try {
                c.close();
            } catch (IOException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
        closeables.clear();
        map1 = null;
    }

    private void mapChecks() {
        if (typeOfMap == TypeOfMap.SIMPLE)
            checkJsonSerialization();
    }

    private void assertArrayValueEquals(ChronicleMap<?, ?> map1, ChronicleMap<?, ?> map2) {

        assertEquals(map1.size(), map2.size(), "deserialized map should have same size as original");

        for (Object key : map1.keySet()) {

            if (map1.valueClass() == byte[].class)
                Assertions.assertArrayEquals((byte[]) map1.get(key), (byte[]) map2.get(key), "deserialized byte array value should match original for key");

            else if (map1.valueClass() == char[].class)
                Assertions.assertArrayEquals((char[]) map1.get(key), (char[]) map2.get(key), "deserialized char array value should match original for key");
            else if (map1.valueClass() == byte[][].class) {
                byte[][] o1 = (byte[][]) map1.get(key);
                byte[][] o2 = (byte[][]) map2.get(key);

                Assertions.assertEquals(o1.length, o2.length, "deserialized 2d byte array should have same outer array length");
                for (int i = 0; i < o1.length; i++) {
                    Assertions.assertArrayEquals(o1[i], o2[i], "deserialized 2d byte array inner array should match at index");
                }
            } else {
                throw new IllegalStateException("unsupported type");
            }

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

            VanillaChronicleMap<?, ?, ?> vanillaMap = (VanillaChronicleMap<?, ?, ?>) map1;
            ChronicleMapBuilder<?, ?> builder = ChronicleMap
                    .of(map1.keyClass(), map1.valueClass())
                    .entriesPerSegment(
                            max(divideRoundUp(map1.size(), vanillaMap.actualSegments), 1))
                    .actualSegments(vanillaMap.actualSegments)
                    .actualChunksPerSegmentTier(vanillaMap.actualChunksPerSegmentTier);
            if (!vanillaMap.constantlySizedEntry) {
                builder.actualChunkSize((int) vanillaMap.chunkSize);
                builder.worstAlignment(vanillaMap.worstAlignment);
            }

            try (ChronicleMap<?, ?> actual = builder.create()) {
                actual.putAll(file);

                if (map1.valueClass() == char[].class ||
                        map1.valueClass() == byte[].class ||
                        map1.valueClass() == byte[][].class) {
                    assertArrayValueEquals(map1, actual);
                } else {
                    Assertions.assertEquals(map1, actual, "deserialized map should equal original map after json roundtrip");
                }
            }
        } catch (IOException e) {
            Assertions.fail("fail");
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
                map1 = builder.create();
                closeables.add(map1);
                return (ChronicleMap<X, Y>) map1;

            case SIMPLE_PERSISTED:
                File file0 = null;
                try {
                    file0 = File.createTempFile("chronicle-map-", ".map");
                    // Be paranoid
                    assertEquals(0, file0.length(), "newly created temp file should be empty before map creation");
                } catch (IOException e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }

                file0.deleteOnExit();
                map1 = builder.createPersistedTo(file0);
                closeables.add(map1);
                closeables.add(file0::delete);

                return (ChronicleMap<X, Y>) map1;

            default:
                throw new IllegalStateException();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testArrayOfString(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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

                assertEquals("Hello", key.getStrAt(1), "first string element in Key1 array should match expected value");
                assertEquals("World", key.getStrAt(2), "second string element in Key1 array should match expected value");
            }

            {
                final I1 key = map.get("Key2");

                assertEquals("Hello2", key.getStrAt(1), "first string element in Key2 array should match expected value");
                assertEquals("World2", key.getStrAt(2), "second string element in Key2 array should match expected value");
            }

            // todo not currently supported for arrays
            // mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testCharArrayValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        int valueSize = 10;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class)
                .averageValue(expected)
                .entries(1);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);

            assertEquals(Chars.asList(expected), Chars.asList(map.get("Key")), "retrieved char array value should match expected characters after roundtrip");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testByteArrayArrayValue(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

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

            assertArrayEquals(value, map.get("Key".getBytes()), "retrieved 2d byte array value should match expected array after roundtrip");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void bondExample(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        long issueDate = parseYYYYMMDD("20130915");
        long maturityDate = parseYYYYMMDD("20140915");
        double coupon = 5.0 / 100; // 5.0%
        double ask930 = 109.2;
        double bid930 = 106.9;
        double ask1030 = 109.7;
        double bid1030 = 107.6;

        ChronicleMapBuilder<String, BondVOInterface> builder = ChronicleMapBuilder.of(String.class, BondVOInterface.class)
                .entries(1)
                .averageKeySize(10);

        try (ChronicleMap<String, BondVOInterface> chm = newInstance(builder)) {
            BondVOInterface bondVO = Values.newNativeReference(BondVOInterface.class);
            try (net.openhft.chronicle.core.io.Closeable c =
                         chm.acquireContext("369604103", bondVO)) {
                bondVO.setIssueDate(issueDate);
                bondVO.setMaturityDate(maturityDate);
                bondVO.setCoupon(coupon);

                BondVOInterface.MarketPx mpx930 = bondVO.getMarketPxIntraDayHistoryAt(0);
                mpx930.setAskPx(ask930);
                mpx930.setBidPx(bid930);

                BondVOInterface.MarketPx mpx1030 = bondVO.getMarketPxIntraDayHistoryAt(1);
                mpx1030.setAskPx(ask1030);
                mpx1030.setBidPx(bid1030);
            }
            BondVOInterface bondRead = Values.newNativeReference(BondVOInterface.class);
            BondVOInterface bond = chm.getUsing("369604103", bondRead);
            assertNotNull(bond, "bond should be present");
            assertEquals(issueDate, bond.getIssueDate(), "issue date");
            assertEquals(maturityDate, bond.getMaturityDate(), "maturity date");
            assertEquals(coupon, bond.getCoupon(), 0.0, "bond coupon should match the stored value");
            assertEquals(ask930, bond.getMarketPxIntraDayHistoryAt(0).getAskPx(), 0.0, "ask 09:30");
            assertEquals(bid930, bond.getMarketPxIntraDayHistoryAt(0).getBidPx(), 0.0, "bid 09:30");
            assertEquals(ask1030, bond.getMarketPxIntraDayHistoryAt(1).getAskPx(), 0.0, "ask 10:30");
            assertEquals(bid1030, bond.getMarketPxIntraDayHistoryAt(1).getBidPx(), 0.0, "bid 10:30");
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testLargeCharSequenceValueWriteOnly(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        int valueSize = 1000000;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class).entries(1)
                .constantValueSizeBySample(expected);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            assertEquals(1, map.size(), "map should contain exactly one entry after put");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testEntrySpanningSeveralChunks(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

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
            assertArrayEquals(expected, map.get("Key"), "large entry spanning multiple chunks should be retrieved correctly");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testKeyValueSizeBySample(TypeOfMap typeOfMap) throws
            IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .averageKeySize("Key".length())
                .averageValueSize("Value".length())
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Key", "Value");
            assertEquals("Value", map.get("Key").toString(), "string value should be retrieved correctly after roundtrip");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testLargeCharSequenceValue(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

        int valueSize = 5_000_000;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class).entries(1)
                .constantValueSizeBySample(expected);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            Assertions.assertArrayEquals(expected, map.get("Key"), "large char array value should be retrieved correctly");
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testStringStringMap(TypeOfMap typeOfMap) throws
            IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .entries(1);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            assertEquals("World", map.get("Hello"), "string value should be retrieved correctly by key");

            assertEquals("New World", map.getMapped("Hello", new PrefixStringFunction("New ")), "getMapped should apply function and return transformed value");
            assertNull(map.getMapped("No key", new PrefixStringFunction("New ")), "getMapped should return null for non-existent key");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testStringStringMapMutableValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .entries(1);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.computeIfPresent("Hello", new StringPrefixUnaryOperator("New "));
            assertEquals("New World", map.get("Hello"), "computeIfPresent should modify value in place");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testCharSequenceMixingKeyTypes(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            map.put("Hello", "World");
            map.put(new StringBuilder("Hello"), "World2");

            Assertions.assertEquals("World2", map.get("Hello").toString(), "charsequence key should retrieve most recently stored value");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testCharSequenceMixingValueTypes(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(2);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.put("Hello2", new StringBuilder("World2"));

            Assertions.assertEquals("World2", map.get("Hello2").toString(), "charsequence value stored as StringBuilder should be retrieved correctly");
            Assertions.assertEquals("World", map.get("Hello").toString(), "charsequence value stored as String should be retrieved correctly");
            mapChecks();
        }
    }

    /**
     * CharSequence is more efficient when object creation is avoided.
     * * The key can only be on heap and variable length serialised.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testCharSequenceCharSequenceMap(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

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
            assertEquals("value-1", map.get("key-1").toString(), "stringbuilder key and value should be stored and retrieved correctly");

            assertEquals(value, map.getUsing(key, value), "getUsing should return same StringBuilder instance provided");
            assertEquals("value-1", value.toString(), "getUsing should populate provided StringBuilder with correct value");
            map.remove("key-1");
            assertNull(map.getUsing(key, value), "getUsing should return null after key removal");

            assertEquals("New World", map.getMapped("Hello", s -> "New " + s), "getMapped with lambda should transform existing value");
            assertNull(map.getMapped("No key",
                    (SerializableFunction<CharSequence, CharSequence>) s -> "New " + s), "getMapped should return null for missing key");

            assertEquals("New World !!", map.computeIfPresent("Hello", (k, s) -> {
                ((StringBuilder) s).append(" !!");
                return "New " + s;
            }).toString(), "computeIfPresent should modify and return transformed value");

            assertEquals("New World !!", map.get("Hello").toString(), "value should be updated in map after computeIfPresent");

            assertEquals("New !!", map.compute("no-key", (k, s) -> {
                assertNull(s, "value should be null for non-existent key in compute function");
                return "New !!";
            }).toString(), "compute should create and return new value for missing key");

            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testAcquireUsingWithCharSequence(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            CharSequence using = new StringBuilder();

            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", using)) {
                assertTrue(using instanceof StringBuilder, "acquireContext should maintain StringBuilder type");
                ((StringBuilder) using).append("Hello World");
            }

            assertEquals("Hello World", map.get("1").toString(), "value modified via acquireContext should be persisted in map");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testGetUsingWithIntValueNoValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<CharSequence, IntValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, IntValue.class)
                .entries(1);

        try (ChronicleMap<CharSequence, IntValue> map = newInstance(builder)) {

            try (ExternalMapQueryContext<CharSequence, IntValue, ?> c = map.queryContext("1")) {
                assertNull(c.entry(), "entry should be null when key does not exist");
            }

            assertNull(map.get("1"), "map should return null for non-existent key");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testAcquireUsingImmutableUsing(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class)
                .entries(1);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {

            IntValue using = Values.newHeapInstance(IntValue.class);
            using.setValue(1);

            assertThrows(IllegalArgumentException.class, () -> {
                try (Closeable c = map.acquireContext(using, "")) {
                    assertTrue(using instanceof IntValue, "acquireContext should maintain IntValue type for immutable key");
                    using.setValue(1);
                }
            });
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testNegativeActualChunkSize(TypeOfMap typeOfMap) {

        this.typeOfMap = typeOfMap;
        assertThrows(IllegalArgumentException.class,
                () -> ChronicleMapBuilder.of(String.class, String.class).actualChunkSize(-1));
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testActualChunksPerSegmentTier(TypeOfMap typeOfMap) {

        this.typeOfMap = typeOfMap;
        assertThrows(IllegalArgumentException.class,
                () -> ChronicleMapBuilder.of(String.class, String.class).actualChunksPerSegmentTier(0));
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testAcquireUsingWithIntValueKeyStringBuilderValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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

            assertEquals("Hello", map.get(key).toString(), "value modified in acquireContext should be retrievable by IntValue key");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testAcquireUsingWithIntValueKey(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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
            assertEquals("Hello", map.get(key).toString(), "map should contain value stored with IntValue key 1");
            mapChecks();
        }
    }

    /**
     * StringValue represents any bean which contains a String Value
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testStringValueStringValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<StringValue, StringValue> builder = ChronicleMapBuilder
                .of(StringValue.class, StringValue.class)
                .entries(10);

        try (ChronicleMap<StringValue, StringValue> map = newInstance(builder)) {
            StringValue key1 = Values.newHeapInstance(StringValue.class);
            final StringValue key2 = Values.newHeapInstance(StringValue.class);
            StringValue value1 = Values.newHeapInstance(StringValue.class);
            final StringValue value2 = Values.newHeapInstance(StringValue.class);

            key1.setValue(new StringBuilder("1"));
            value1.setValue("11");
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            key2.setValue("2");
            value2.setValue(new StringBuilder("22"));
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            mapChecks();

            StringBuilder sb = new StringBuilder();
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in StringValue map after initial put");
                StringValue v = entry.value().get();
                assertEquals("11", v.getValue().toString(), "StringValue retrieved from entry should contain expected string content");
                v.getUsingValue(sb);
                assertEquals("11", sb.toString(), "getUsingValue should populate StringBuilder with correct value");
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in StringValue map after initial put");
                StringValue v = entry.value().get();
                assertEquals("22", v.getValue().toString(), "StringValue retrieved from entry should contain expected string content");
                v.getUsingValue(sb);
                assertEquals("22", sb.toString(), "getUsingValue should populate StringBuilder with correct value");
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query");
                StringValue v = entry.value().get();
                assertEquals("11", v.getValue().toString(), "StringValue retrieved from entry should contain expected string content");
                v.getUsingValue(sb);
                assertEquals("11", sb.toString(), "getUsingValue should populate StringBuilder with correct value");
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query");
                StringValue v = entry.value().get();
                assertEquals("22", v.getValue().toString(), "StringValue retrieved from entry should contain expected string content");
                v.getUsingValue(sb);
                assertEquals("22", sb.toString(), "getUsingValue should populate StringBuilder with correct value");
            }

            key1.setValue("3");
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            key2.setValue("4");
            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals("", value1.getValue().toString(), "newly acquired value should be empty before initialization");
                value1.getUsingValue(sb);
                assertEquals("", sb.toString(), "newly acquired value should populate StringBuilder as empty");
                sb.append(123);
                value1.setValue(sb);
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals("123", value2.getValue().toString(), "acquired value should contain previously set content");
                value2.setValue(value2.getValue().toString() + '4');
                assertEquals("1234", value2.getValue().toString(), "value should reflect modification after append operation");
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key1)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification");
                assertEquals("1234", entry.value().get().getValue().toString(), "persisted value should match modified value from acquireContext");
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals("", value2.getValue().toString(), "newly acquired value should be empty before initialization");
                value2.getUsingValue(sb);
                assertEquals("", sb.toString(), "newly acquired value should populate StringBuilder as empty");
                sb.append(123);
                value2.setValue(sb);
            }

            mapChecks();

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals("123", value1.getValue().toString(), "acquired value should contain previously set content");
                value1.setValue(value1.getValue().toString() + '4');
                assertEquals("1234", value1.getValue().toString(), "value should reflect modification after append operation");
            }

            mapChecks();

            try (ExternalMapQueryContext<StringValue, StringValue, ?> c = map.queryContext(key2)) {
                MapEntry<StringValue, StringValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification");
                assertEquals("1234", entry.value().get().getValue().toString(), "persisted value should match modified value from acquireContext");
            }

            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntegerIntegerMap(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(10);

        try (ChronicleMap<Integer, Integer> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            Integer key1;
            final Integer key2;
            Integer value1;
            final Integer value2;

            key1 = 1;
            value1 = 11;
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            key2 = 2;
            value2 = 22;
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            assertEquals((Integer) 11, map.get(key1), "integer value for key1 should be retrievable after puts");
            assertEquals((Integer) 22, map.get(key2), "integer value for key2 should be retrievable after puts");
            assertNull(map.get(3), "map should return null for non-existent key 3");
            assertNull(map.get(4), "map should return null for non-existent key 4");

            mapChecks();

            assertEquals((Integer) 110, map.getMapped(1, new SerializableFunction<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }), "getMapped should apply transformation function to existing integer value");

            mapChecks();

            assertNull(map.getMapped(-1, new SerializableFunction<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }), "getMapped should return null for non-existent integer key");

            mapChecks();

            try {
                map.computeIfPresent(1, (k, s) -> s + 1);
            } catch (Exception todoMoreSpecificException) {
                assertNotNull(todoMoreSpecificException, "computeIfPresent should throw exception for unsupported operation");
            }
            mapChecks();

        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testLongLongMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<Long, Long> builder = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .entries(10);

        try (ChronicleMap<Long, Long> map = newInstance(builder)) {
            //            assertEquals(16, entrySize(map));
            //            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            map.put(1L, 11L);
            assertEquals((Long) 11L, map.get(1L), "long value for key 1 should be retrievable after puts");

            map.put(2L, 22L);
            assertEquals((Long) 22L, map.get(2L), "long value for key 2 should be retrievable after puts");

            assertNull(map.get(3L), "map should return null for non-existent key 3");
            assertNull(map.get(4L), "map should return null for non-existent key 4");

            mapChecks();

            assertEquals((Long) 110L, map.getMapped(1L, new SerializableFunction<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }), "getMapped should apply transformation function to existing long value");
            assertNull(map.getMapped(-1L, (SerializableFunction<Long, Long>) s -> 10 * s), "getMapped should return null for non-existent long key");

            mapChecks();

            try {
                map.computeIfPresent(1L, (k, s) -> s + 1);
            } catch (Exception todoMoreSpecificException) {
                assertNotNull(todoMoreSpecificException, "computeIfPresent should throw exception for unsupported operation");
            }

            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testDoubleDoubleMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<Double, Double> builder = ChronicleMapBuilder
                .of(Double.class, Double.class)
                .entries(10);

        try (ChronicleMap<Double, Double> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            map.put(1.0, 11.0);
            assertEquals((Double) 11.0, map.get(1.0), "double value for key 1.0 should be retrievable after puts");

            map.put(2.0, 22.0);
            assertEquals((Double) 22.0, map.get(2.0), "double value for key 2.0 should be retrievable after puts");

            assertNull(map.get(3.0), "map should return null for non-existent key 3.0");
            assertNull(map.get(4.0), "map should return null for non-existent key 4.0");

            assertEquals((Double) 110.0, map.getMapped(1.0, new SerializableFunction<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }), "getMapped should apply transformation function to existing double value");
            assertNull(map.getMapped(-1.0, (SerializableFunction<Double, Double>) s -> 10 * s), "getMapped should return null for non-existent double key");

            try {
                map.computeIfPresent(1.0, (k, s) -> s + 1);

            } catch (Exception todoMoreSpecificException) {
                assertNotNull(todoMoreSpecificException, "computeIfPresent should throw exception for unsupported operation");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testByteArrayByteArrayMap(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<byte[], byte[]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[].class).averageKeySize(4).averageValueSize(4)
                .entries(1000);

        try (ChronicleMap<byte[], byte[]> map = newInstance(builder)) {
            byte[] key1 = {1, 1, 1, 1};
            final byte[] key2 = {2, 2, 2, 2};
            byte[] value1 = {11, 11, 11, 11};
            final byte[] value2 = {22, 22, 22, 22};
            assertNull(map.put(key1, value1), "put should return null for new key insertion");
            assertArrayEquals(value1, map.put(key1, value2), "put should return previous value when replacing existing key");
            assertArrayEquals(value2, map.get(key1), "byte array value should match most recently stored value for key1");
            assertNull(map.get(key2), "map should return null for non-existent key2");

            map.put(key1, value1);

            assertArrayEquals(new byte[]{11, 11}, map.getMapped(key1, new SerializableFunction<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            }), "getMapped should apply transformation function to byte array value");
            assertNull(map.getMapped(key2, new SerializableFunction<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            }), "getMapped should return null for non-existent byte array key");

            assertArrayEquals(new byte[]{12, 10}, map.computeIfPresent(key1, (k, s) -> {
                s[0]++;
                s[1]--;
                return Arrays.copyOf(s, 2);
            }), "computeIfPresent should modify byte array in-place and return result");

            byte[] a2 = map.get(key1);
            assertArrayEquals(new byte[]{12, 10}, a2, "persisted byte array should reflect computeIfPresent modifications");

        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testByteBufferByteBufferDefaultKeyValueMarshaller(TypeOfMap typeOfMap) throws
            IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(8)
                .averageValueSize(8)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            final ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            final ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            final ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11});
            final ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22});
            assertNull(map.put(key1, value1), "put should return null for new key insertion");
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2), "map should return null for non-existent key2");

            map.put(key1, value1);

            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testByteBufferByteBufferMap(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(8)
                .averageValueSize(8)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1}).order(ByteOrder.nativeOrder());
            final ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2}).order(ByteOrder.nativeOrder());
            ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11}).order(ByteOrder.nativeOrder());
            final ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22}).order(ByteOrder.nativeOrder());
            assertNull(map.put(key1, value1), "put should return null for new key insertion");
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2), "map should return null for non-existent key2");

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
            assertNull(map.getMapped(key2, function), "getMapped should return null for non-existent key");
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
            ByteBuffer valueA = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
            ByteBuffer valueB = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
            //            assertBBEquals(value1, valueA);
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key1)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in ByteBuffer map using valueA");
                assertBBEquals(value1, entry.value().getUsing(valueA));
            }
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key2)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in ByteBuffer map using valueA");
                assertBBEquals(value2, entry.value().getUsing(valueA));
            }

            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key1)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in ByteBuffer map using valueB");
                assertBBEquals(value1, entry.value().getUsing(valueB));
            }
            try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> c = map.queryContext(key2)) {
                MapEntry<ByteBuffer, ByteBuffer> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in ByteBuffer map using valueB");
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
                assertNotNull(entry, "entry should exist for key1 after acquireContext modifications in ByteBuffer map");

                ByteBuffer bb1 = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
                bb1.put(value1);
                bb1.putShort((short) 12345);
                bb1.flip();
                assertBBEquals(bb1, entry.value().getUsing(valueA));
            }

            mapChecks();
        }
    }

    @SuppressWarnings("cast")
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testByteBufferDirectByteBufferMap(TypeOfMap typeOfMap)
            throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .averageKeySize(5)
                .averageValueSize(5)
                .entries(1000);

        try (ChronicleMap<ByteBuffer, ByteBuffer> map = newInstance(builder)) {

            final boolean useOnHeapKey = true; // Either should work
            final ByteBuffer key1 = useOnHeapKey
                    ? ByteBuffer.wrap(new byte[]{1, 1, 1, 1})

                    : ((ByteBuffer) (ByteBuffer.allocateDirect(4)
                    .put(new byte[]{1, 1, 1, 1})
                    .flip()))
                    .asReadOnlyBuffer();

            final ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            // Apparently, asReadOnlyBuffer cannot be used as keys because the backing array cannot be exposed;

            final ByteBuffer value1 = ((ByteBuffer) ByteBuffer.allocateDirect(4)
                    .put(new byte[]{11, 11, 11, 11})
                    .flip())
                    .asReadOnlyBuffer();

            final ByteBuffer value2 = ((ByteBuffer) ByteBuffer.allocateDirect(4)
                    .put(new byte[]{22, 22, 22, 22})
                    .flip())
                    .asReadOnlyBuffer();

            assertNull(map.put(key1, value1), "put should return null for new key insertion");
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));

            assertNull(map.get(key2), "map should return null for non-existent key2");
            assertBBEquals(value2, map.put(key1, value1));
            assertBBEquals(value1, map.get(key1));

            mapChecks();

            final SerializableFunction<ByteBuffer, ByteBuffer> function =
                    s -> {
                        ByteBuffer slice = s.slice();
                        slice.limit(2);
                        return slice;
                    };
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.getMapped(key1, function));
            assertNull(map.getMapped(key2, function), "getMapped should return null for non-existent key");
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
        assertEquals(bb1.remaining(), bb2.remaining(), "bytebuffers should have same number of remaining bytes");
        for (int i = 0; i < bb1.remaining(); i++)
            assertEquals(bb1.get(bb1.position() + i), bb2.get(bb2.position() + i), "bytebuffer bytes should match at each position");
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueIntValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, IntValue> builder = ChronicleMapBuilder
                .of(IntValue.class, IntValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, IntValue> map = newInstance(builder)) {
            // this may change due to alignment
            //            assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            final IntValue key1 = Values.newHeapInstance(IntValue.class);
            final IntValue key2 = Values.newHeapInstance(IntValue.class);
            final IntValue value1 = Values.newHeapInstance(IntValue.class);
            final IntValue value2 = Values.newHeapInstance(IntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in IntValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO review -- the previous version of this block:
            // acquiring for value1, comparing value2 -- as intended?
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in IntValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in IntValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in IntValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in IntValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<IntValue, IntValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, IntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in IntValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }
            mapChecks();

        }
    }

    /**
     * For unsigned int -> unsigned int entries, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testUnsignedIntValueUnsignedIntValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<UnsignedIntValue, UnsignedIntValue> builder = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class)
                .entries(10);

        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            UnsignedIntValue key1 = Values.newHeapInstance(UnsignedIntValue.class);
            UnsignedIntValue value1 = Values.newHeapInstance(UnsignedIntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            key1 = Values.newHeapInstance(UnsignedIntValue.class);
            value1 = Values.newHeapInstance(UnsignedIntValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            UnsignedIntValue key2 = Values.newHeapInstance(UnsignedIntValue.class);
            UnsignedIntValue value2 = Values.newHeapInstance(UnsignedIntValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in UnsignedIntValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO review suspicious block
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in UnsignedIntValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in UnsignedIntValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in UnsignedIntValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in UnsignedIntValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<UnsignedIntValue, UnsignedIntValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<UnsignedIntValue, UnsignedIntValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in UnsignedIntValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueShortValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, ShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, ShortValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, ShortValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(6, entrySize(map));

            //     assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            final IntValue key1 = Values.newHeapInstance(IntValue.class);
            final IntValue key2 = Values.newHeapInstance(IntValue.class);
            final ShortValue value1 = Values.newHeapInstance(ShortValue.class);
            final ShortValue value2 = Values.newHeapInstance(ShortValue.class);

            key1.setValue(1);
            value1.setValue((short) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            key2.setValue(2);
            value2.setValue((short) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in ShortValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO the same as above.
            //            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
            //                assertTrue(rc.present());
            //                assertEquals(22, value2.getValue());
            //            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in ShortValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in ShortValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in ShortValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, ShortValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue((short) 123);
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
                value2.addValue((short) (1230 - 123));
                assertEquals(1230, value2.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in ShortValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue((short) 123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue((short) (1230 - 123));
                assertEquals(1230, value1.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, ShortValue, ?> c =
                         map.queryContext(key2)) {
                MapEntry<?, ShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in ShortValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }
            mapChecks();
        }
    }

    /**
     * For int -> unsigned short values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueUnsignedShortValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, UnsignedShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedShortValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, UnsignedShortValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            UnsignedShortValue value1 = Values.newHeapInstance(UnsignedShortValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            IntValue key2 = Values.newHeapInstance(IntValue.class);
            UnsignedShortValue value2 = Values.newHeapInstance(UnsignedShortValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in UnsignedShortValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO the same as above.
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in UnsignedShortValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in UnsignedShortValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in UnsignedShortValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c =
                         map.queryContext(key1)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in UnsignedShortValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, UnsignedShortValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedShortValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in UnsignedShortValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueCharValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, CharValue> builder = ChronicleMapBuilder
                .of(IntValue.class, CharValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, CharValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");
            IntValue key1 = Values.newHeapInstance(IntValue.class);
            CharValue value1 = Values.newHeapInstance(CharValue.class);

            key1.setValue(1);
            value1.setValue((char) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            IntValue key2 = Values.newHeapInstance(IntValue.class);
            CharValue value2 = Values.newHeapInstance(CharValue.class);

            key2.setValue(2);
            value2.setValue((char) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in CharValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO The same as above
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in CharValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in CharValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in CharValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, CharValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals('\0', value1.getValue(), "acquired char value should be null character when key does not exist");
                value1.setValue('@');
                assertEquals('@', value1.getValue(), "char value should be '@' after setting");
            }
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext(key1, value2)) {
                assertEquals('@', value2.getValue(), "acquired char value should be '@' matching previously stored value");
                value2.setValue('#');
                assertEquals('#', value2.getValue(), "char value should be '#' after updating");
            }
            try (ExternalMapQueryContext<IntValue, CharValue, ?> c = map.queryContext(key1)) {
                MapEntry<IntValue, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in IntValue-CharValue map");
                assertEquals('#', entry.value().get().getValue(), "key1 charvalue should contain '#' character after modification");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals('\0', value2.getValue(), "acquired char value should be null character when key does not exist");
                value2.setValue(';');
                assertEquals(';', value2.getValue(), "char value should be ';' after setting");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(';', value1.getValue(), "acquired char value should be ';' matching previously stored value");
                value1.setValue('[');
                assertEquals('[', value1.getValue(), "char value should be '[' after updating");
            }
            try (ExternalMapQueryContext<IntValue, CharValue, ?> c = map.queryContext(key2)) {
                MapEntry<IntValue, CharValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in IntValue-CharValue map");
                assertEquals('[', entry.value().get().getValue(), "key2 charvalue should contain '[' character after modification");
            }
        }
    }

    /**
     * For int-> byte entries, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueUnsignedByteMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, UnsignedByteValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedByteValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, UnsignedByteValue> map = newInstance(builder)) {

            // TODO should be 5, but shorter fields based on range doesn't seem to be implemented
            // on data value generation level yet
            //assertEquals(8, entrySize(map)); this may change due to alignmented
            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");

            IntValue key1 = Values.newHeapInstance(IntValue.class);
            UnsignedByteValue value1 = Values.newHeapInstance(UnsignedByteValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            IntValue key2 = Values.newHeapInstance(IntValue.class);
            UnsignedByteValue value2 = Values.newHeapInstance(UnsignedByteValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in UnsignedByteValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO the same as above
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in UnsignedByteValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in UnsignedByteValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in UnsignedByteValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue(234);
                assertEquals(234, value1.getValue(), "unsigned byte value should be 234 after adding 234");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(234, value2.getValue(), "acquired unsigned byte value should be 234 matching previously stored value");
                value2.addValue(-100);
                assertEquals(134, value2.getValue(), "unsigned byte value should be 134 after subtracting 100");
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in UnsignedByteValue map");
                assertEquals(134, entry.value().get().getValue(), "key1 unsigned byte value should be 134 after subtraction operation");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue((byte) 123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue((byte) -111);
                assertEquals(12, value1.getValue(), "unsigned byte value should be 12 after subtracting 111");
            }
            try (ExternalMapQueryContext<?, UnsignedByteValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, UnsignedByteValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in UnsignedByteValue map");
                assertEquals(12, entry.value().get().getValue(), "key2 unsigned byte value should be 12 after subtraction operation");
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testIntValueBooleanValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<IntValue, BooleanValue> builder = ChronicleMapBuilder
                .of(IntValue.class, BooleanValue.class)
                .entries(10);

        try (ChronicleMap<IntValue, BooleanValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");

            IntValue key1 = Values.newHeapInstance(IntValue.class);
            BooleanValue value1 = Values.newHeapInstance(BooleanValue.class);

            key1.setValue(1);
            value1.setValue(true);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            IntValue key2 = Values.newHeapInstance(IntValue.class);
            BooleanValue value2 = Values.newHeapInstance(BooleanValue.class);

            key2.setValue(2);
            value2.setValue(false);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in BooleanValue map after initial put");
                assertTrue(entry.value().get().getValue(), "key1 boolean value should be true as initially stored");
            }
            // TODO the same as above. copy paste, copy paste, copy-paste...
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in BooleanValue map after initial put");
                assertFalse(entry.value().get().getValue(), "key2 boolean value should be false as initially stored");
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in BooleanValue map");
                assertTrue(entry.value().get().getValue(), "key1 boolean value should remain true on repeated query");
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in BooleanValue map");
                assertFalse(entry.value().get().getValue(), "key2 boolean value should remain false on repeated query");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertFalse(value1.getValue(), "acquired boolean value should be false for new key");
                value1.setValue(true);
                assertTrue(value1.getValue(), "boolean value should be true after setting");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertTrue(value2.getValue(), "acquired boolean value should be true matching previously stored value");
                value2.setValue(false);
                assertFalse(value2.getValue(), "boolean value should be false after updating");
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in BooleanValue map");
                assertFalse(entry.value().get().getValue(), "key1 boolean value should be false after last modification");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertFalse(value2.getValue(), "acquired boolean value should be false for new key");
                value2.setValue(true);
                assertTrue(value2.getValue(), "boolean value should be true after setting");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertTrue(value1.getValue(), "acquired boolean value should be true matching previously stored value");
                value1.setValue(false);
                assertFalse(value1.getValue(), "boolean value should be false after updating");
            }
            try (ExternalMapQueryContext<?, BooleanValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, BooleanValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in BooleanValue map");
                assertFalse(entry.value().get().getValue(), "key2 boolean value should be false after last modification");
            }
            mapChecks();
        }
    }

    /**
     * For float values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testFloatValueFloatValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<FloatValue, FloatValue> builder = ChronicleMapBuilder
                .of(FloatValue.class, FloatValue.class)
                .entries(10);

        try (ChronicleMap<FloatValue, FloatValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");

            FloatValue key1 = Values.newHeapInstance(FloatValue.class);
            FloatValue value1 = Values.newHeapInstance(FloatValue.class);

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1), "value retrieved for key1 should match stored value");

            FloatValue key2 = Values.newHeapInstance(FloatValue.class);
            FloatValue value2 = Values.newHeapInstance(FloatValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in FloatValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), 0, "key1 float value should be 11 as initially stored");
            }
            // TODO see above
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in FloatValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), 0, "key2 float value should be 22 as initially stored");
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in FloatValue map");
                assertEquals(11, entry.value().get().getValue(), 0, "key1 float value should remain 11 on repeated query");
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in FloatValue map");
                assertEquals(22, entry.value().get().getValue(), 0, "key2 float value should remain 22 on repeated query");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0, "acquired float value should be zero for new key");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0, "float value should be 123 after adding 123");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0, "acquired float value should be 123 matching previously stored value");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0, "float value should be 1230 after adding 1107");
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in FloatValue map");
                assertEquals(1230, entry.value().get().getValue(), 0, "key1 float value should be 1230 after addition operations");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0, "acquired float value should be zero for new key");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0, "float value should be 123 after adding 123");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0, "acquired float value should be 123 matching previously stored value");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0, "float value should be 1230 after adding 1107");
            }
            try (ExternalMapQueryContext<?, FloatValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, FloatValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in FloatValue map");
                assertEquals(1230, entry.value().get().getValue(), 0, "key2 float value should be 1230 after addition operations");
            }
            mapChecks();
        }
    }

    /**
     * For double values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testDoubleValueDoubleValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<DoubleValue, DoubleValue> builder = ChronicleMapBuilder
                .of(DoubleValue.class, DoubleValue.class)
                .entries(10);

        try (ChronicleMap<DoubleValue, DoubleValue> map = newInstance(builder)) {

            // this may change due to alignment
            //assertEquals(16, entrySize(map));

            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");

            DoubleValue key1 = Values.newHeapInstance(DoubleValue.class);
            DoubleValue value1 = Values.newHeapInstance(DoubleValue.class);

            key1.setValue(1);
            value1.setValue(11);
            assertNull(map.get(key1), "map should return null after removing key1");

            map.put(key1, value1);
            DoubleValue v2 = map.get(key1);
            assertEquals(value1, v2, "v2");

            DoubleValue key2 = Values.newHeapInstance(DoubleValue.class);
            DoubleValue value2 = Values.newHeapInstance(DoubleValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in DoubleValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), 0, "key1 double value should be 11 as initially stored");
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in DoubleValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), 0, "key2 double value should be 22 as initially stored");
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in DoubleValue map");
                assertEquals(11, entry.value().get().getValue(), 0, "key1 double value should remain 11 on repeated query");
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in DoubleValue map");
                assertEquals(22, entry.value().get().getValue(), 0, "key2 double value should remain 22 on repeated query");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0, "acquired double value should be zero for new key");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0, "double value should be 123 after adding 123");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0, "acquired double value should be 123 matching previously stored value");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0, "double value should be 1230 after adding 1107");
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in DoubleValue map");
                assertEquals(1230, entry.value().get().getValue(), 0, "key1 double value should be 1230 after addition operations");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0, "acquired double value should be zero for new key");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0, "double value should be 123 after adding 123");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0, "acquired double value should be 123 matching previously stored value");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0, "double value should be 1230 after adding 1107");
            }
            try (ExternalMapQueryContext<?, DoubleValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, DoubleValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in DoubleValue map");
                assertEquals(1230, entry.value().get().getValue(), 0, "key2 double value should be 1230 after addition operations");
            }
            mapChecks();
        }
    }

    /**
     * For long values, the key can be on heap or off heap.
     */
    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testLongValueLongValueMap(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMapBuilder
                .of(LongValue.class, LongValue.class)
                .entries(10);

        try (ChronicleMap<LongValue, LongValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(16, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap<?, ?, ?>) map).maxChunksPerEntry, "constant-sized entries should require only one chunk per entry");

            LongValue key1 = Values.newHeapInstance(LongValue.class);
            LongValue value1 = Values.newHeapInstance(LongValue.class);

            key1.setValue(1);
            value1.setValue(11);
            assertNull(map.get(key1), "map should return null after removing key1");
            map.put(key1, value1);

            LongValue key2 = Values.newHeapInstance(LongValue.class);
            LongValue value2 = Values.newHeapInstance(LongValue.class);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2), "value retrieved for key2 should match stored value");

            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 in LongValue map after initial put");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            // TODO see above
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 in LongValue map after initial put");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 on repeated query in LongValue map");
                assertEquals(11, entry.value().get().getValue(), "value retrieved from entry should equal 11");
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 on repeated query in LongValue map");
                assertEquals(22, entry.value().get().getValue(), "value retrieved from entry should equal 22");
            }
            key1.setValue(3);
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }
            key2.setValue(4);
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                assertNotNull(c.absentEntry(), "absentEntry should be available for non-existent key");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), "newly acquired value should be initialized to zero");
                value1.addValue(123);
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key1)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key1 after acquireContext modification in LongValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), "newly acquired value should be initialized to zero");
                value2.addValue(123);
                assertEquals(123, value2.getValue(), "acquired value should contain previously stored content");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), "acquired value should contain previously stored content");
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), "value should reflect in-place modification after multiplication");
            }
            try (ExternalMapQueryContext<?, LongValue, ?> c = map.queryContext(key2)) {
                MapEntry<?, LongValue> entry = c.entry();
                assertNotNull(entry, "entry should exist for key2 after acquireContext modification in LongValue map");
                assertEquals(1230, entry.value().get().getValue(), "persisted value should reflect modifications from acquireContext");
            }
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testListValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

        ChronicleMapBuilder<String, List<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<List<String>>) (Class) List.class)
                .entries(2)
                .valueMarshaller(ListMarshaller.of(
                        new StringBytesReader(), CharSequenceBytesWriter.INSTANCE));

        try (ChronicleMap<String, List<String>> map = newInstance(builder)) {
            map.put("1", Collections.emptyList());
            map.put("2", Collections.singletonList("two-A"));

            List<String> list1 = new ArrayList<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", list1)) {
                list1.add("one");
                assertEquals(Collections.singletonList("one"), list1, "list should contain newly added value after acquireContext modification");
            }
            List<String> list2 = new ArrayList<>();
            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("1")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '1' in List value map after initial put");
                assertEquals(Collections.singletonList("one"), entry.value().getUsing(list2), "entry value should match expected list when retrieved using getUsing");
            }

            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("2")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '2' in List value map in read context");
                entry.value().getUsing(list2);
                list2.add("two-B");     // this is not written as it only a read context
                assertEquals(asList("two-A", "two-B"), list2, "list should contain both original and newly added values in read context");
            }

            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", list1)) {
                list1.add("two-C");
                assertEquals(asList("two-A", "two-C"), list1, "list should contain original value and newly added value after acquireContext modification");
            }

            try (ExternalMapQueryContext<String, List<String>, ?> c = map.queryContext("2")) {
                MapEntry<String, List<String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '2' after acquireContext modification in List value map");
                assertEquals(asList("two-A", "two-C"), entry.value().getUsing(list2), "entry value should contain modified list values when retrieved using getUsing");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testSetValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;
        ChronicleMapBuilder<String, Set<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Set<String>>) (Class) Set.class)
                .entries(10)
                .valueMarshaller(SetMarshaller.of(
                        new StringBytesReader(), CharSequenceBytesWriter.INSTANCE));

        try (ChronicleMap<String, Set<String>> map = newInstance(builder)) {
            map.put("1", Collections.emptySet());
            map.put("2", new LinkedHashSet<>(Collections.singletonList("one")));

            Set<String> list1 = new LinkedHashSet<>();
            try (net.openhft.chronicle.core.io.Closeable c = map.acquireContext("1", list1)) {
                list1.add("two");
                assertEquals(new LinkedHashSet<>(Collections.singletonList("two")), list1, "set should contain newly added value after acquireContext modification");
            }
            Set<String> list2 = new LinkedHashSet<>();
            try (ExternalMapQueryContext<String, Set<String>, ?> c = map.queryContext("1")) {
                MapEntry<String, Set<String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '1' after acquireContext modification in Set value map");
                assertEquals(new LinkedHashSet<>(Collections.singletonList("two")), entry.value().getUsing(list2), "entry value should match expected set when retrieved using getUsing");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", list1)) {
                list1.add("three");
                assertEquals(new LinkedHashSet<String>(asList("one", "three")), list1, "set should contain original and newly added values after acquireContext modification");
            }
            try (ExternalMapQueryContext<String, Set<String>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Set<String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '2' after acquireContext modification in Set value map");
                assertEquals(new LinkedHashSet<>(asList("one", "three")),
                        entry.value().getUsing(list2), "entry value should contain both original and added set values when retrieved using getUsing");
            }

            int keySum = 0;
            int valueCount = 0;
            for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                keySum += entry.getKey().length();
                valueCount += entry.getValue().size();
            }
            assertTrue(keySum > 0, "keySum > 0");
            assertTrue(valueCount > 0, "valueCount > 0");

            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testMapStringStringValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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
                assertEquals(mapOf("two", "bi"), map1, "map should contain newly added key-value pair after acquireContext modification");
            }
            Map<String, String> map2 = new LinkedHashMap<>();
            try (ExternalMapQueryContext<String, Map<String, String>, ?> c =
                         map.queryContext("1")) {
                MapEntry<String, Map<String, String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '1' after acquireContext modification in Map<String,String> value");
                assertEquals(mapOf("two", "bi"), entry.value().getUsing(map2), "entry value should match expected map contents when retrieved using getUsing");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", map1)) {
                map1.put("three", "tri");
                assertEquals(mapOf("one", "uni", "three", "tri"), map1, "map should contain both original and newly added key-value pairs after acquireContext modification");
            }
            try (ExternalMapQueryContext<String, Map<String, String>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Map<String, String>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '2' after acquireContext modification in Map<String,String> value");
                assertEquals(mapOf("one", "uni", "three", "tri"), entry.value().getUsing(map2), "entry value should contain both original and added key-value pairs when retrieved using getUsing");
            }
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testMapStringIntegerValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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
                assertEquals(mapOf("two", 2), map1, "map should contain newly added String-Integer pair after acquireContext modification");
            }
            Map<String, Integer> map2 = new LinkedHashMap<>();
            try (ExternalMapQueryContext<String, Map<String, Integer>, ?> c =
                         map.queryContext("1")) {
                MapEntry<String, Map<String, Integer>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '1' after acquireContext modification in Map<String,Integer> value");
                assertEquals(mapOf("two", 2), entry.value().getUsing(map2), "entry value should match expected String-Integer map when retrieved using getUsing");
            }
            try (net.openhft.chronicle.core.io.Closeable c =
                         map.acquireContext("2", map1)) {
                map1.put("three", 3);
                assertEquals(mapOf("one", 1, "three", 3), map1, "map should contain both original and newly added String-Integer pairs after acquireContext modification");
            }
            try (ExternalMapQueryContext<String, Map<String, Integer>, ?> c =
                         map.queryContext("2")) {
                MapEntry<String, Map<String, Integer>> entry = c.entry();
                assertNotNull(entry, "entry should exist for key '2' after acquireContext modification in Map<String,Integer> value");
                assertEquals(mapOf("one", 1, "three", 3), entry.value().getUsing(map2), "entry value should contain both original and added String-Integer pairs when retrieved using getUsing");
            }
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testMapStringIntegerValueWithoutListMarshallers(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;
        ChronicleMapBuilder<String, Map<String, Integer>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class)
                .averageKey("2")
                .averageValue(mapOf("two", 2))
                .entries(2);
        try (ChronicleMap<String, Map<String, Integer>> map = newInstance(builder)) {
            map.put("1", Collections.emptyMap());
            map.put("2", mapOf("two", 2));

            assertEquals(mapOf("two", 2), map.get("2"), "retrieved map value should contain expected key-value pair");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testGeneratedDataValue(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;

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

            assertEquals(2, map.get("1").getLong(), "long field of retrieved bean should match expected value");
            assertEquals("Hello world", map.get("1").getInnerAt(1).getMessage(), "nested bean message should match expected value");
            mapChecks();
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testBytesMarshallable(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;
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
                assertNotNull(map.entrySet().toString(), "entrySet toString should not be null");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testBytesMarshallable2(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;
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
                assertNotNull(map.entrySet().toString(), "entrySet toString should not be null");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("typeOfMaps")
    public void testBytesMarshallable3(TypeOfMap typeOfMap) throws IOException {

        this.typeOfMap = typeOfMap;
        BytesMarshallableReaderWriter<IData.Data> bmwr = new BytesMarshallableReaderWriter<>(IData.Data.class);
        ChronicleMapBuilder<IData.Data, IData.Data> builder = ChronicleMapBuilder
                .of(IData.Data.class, IData.Data.class)
                .keyMarshaller(bmwr)
                .valueMarshaller(bmwr)
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
                assertNotNull(map.entrySet().toString(), "entrySet toString should not be null");
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

    @SuppressWarnings("serial")
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
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return prefix;
        }
    }

    @SuppressWarnings("serial")
    private static class StringPrefixUnaryOperator
            implements BiFunction<String, String, String>, Serializable {

        private final String prefix;

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
