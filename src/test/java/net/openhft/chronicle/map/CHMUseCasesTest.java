package net.openhft.chronicle.map;

import com.google.common.primitives.Chars;
import net.openhft.chronicle.hash.function.Function;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.constraints.MaxSize;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.values.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static net.openhft.chronicle.map.StatelessClientTest.localClient;
import static net.openhft.chronicle.map.fromdocs.OpenJDKAndHashMapExamplesTest.parseYYYYMMDD;
import static org.junit.Assert.*;

/**
 * This test enumerates common use cases for keys and values.
 */
@RunWith(value = Parameterized.class)
public class CHMUseCasesTest {

    private static final String TMP = System.getProperty("java.io.tmpdir");


    enum TypeOfMap {STATELESS,SIMPLE, SIMPLE_PERSISTED, REPLICATED}

    private final TypeOfMap typeOfMap;

    Collection<Closeable> closeables = new ArrayList<Closeable>();

    public CHMUseCasesTest(TypeOfMap typeOfMap) {
        this.typeOfMap = typeOfMap;
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

    ChronicleMap map1;
    ChronicleMap map2;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {
                        TypeOfMap.SIMPLE
                },

                {
                        TypeOfMap.REPLICATED
                },

                //  it pointless to run these test as the Function and UnaryOperator are not
                // serializable as inner classes adn the getUsingLock is not supported by the
                // stateless client
                {
                        TypeOfMap.STATELESS
                },
                {
                        TypeOfMap.SIMPLE_PERSISTED
                }
        });

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
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private void mapChecks() {
        if (typeOfMap == TypeOfMap.REPLICATED) {

            // see HCOLL-265 Chronicle Maps with Identical char[] values are not equal1
            if (map1.valueClass() == char[].class ||
                    map1.valueClass() == byte[].class ||
                    map1.valueClass() == byte[][].class) {

                waitTillEqual(5000);

                assertArrayValueEquals(map1, map2);
                if (typeOfMap == TypeOfMap.SIMPLE)
                    checkJsonSerilization();

                return;
            }


            // see HCOLL-265 Chronicle Maps with Identical char[] values are not equal1
            if (map1.keyClass() == char[].class ||
                    map1.keyClass() == byte[][].class) {

                return;
            }


            if (ByteBuffer.class.isAssignableFrom(map1.valueClass()) ||
                    ByteBuffer.class.isAssignableFrom(map1.keyClass()))
                return;


            waitTillEqual(5000);
            assertEquals(map1, map2);
        }

        if (typeOfMap == TypeOfMap.SIMPLE)
            checkJsonSerilization();

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


    private void checkJsonSerilization() {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();
        try {

            map1.getAll(file);

            VanillaChronicleMap vanillaMap = (VanillaChronicleMap) map1;
            ChronicleMapBuilder builder = ChronicleMapBuilder.of(map1.keyClass(),
                    map1.valueClass())
                    .entriesPerSegment(vanillaMap.entriesPerSegment)
                    .actualSegments(vanillaMap.actualSegments)
                    .actualChunksPerSegment(vanillaMap.actualChunksPerSegment);
            if (!vanillaMap.constantlySizedEntry)
                builder.actualChunkSize((int) vanillaMap.chunkSize);
            try (ChronicleMap<Integer, Double> actual = builder.create()) {
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
        switch (typeOfMap) {

            case SIMPLE:
                map2 = null;
                map1 = builder.create();
                closeables.add(map1);
                return map1;

            case SIMPLE_PERSISTED:
                final File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".map");
                file.deleteOnExit();
                map1 = builder.createPersistedTo(file);
                closeables.add(map1);
                closeables.add(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        file.delete();
                    }
                });

                return map1;

            case REPLICATED: {

                map2 = null;
                {
                    final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig
                            .of(8086).name("server")
                            .heartBeatInterval(1, TimeUnit.SECONDS)
                            .tcpBufferSize(1024 * 64);


                    map2 = builder
                            .replication((byte) 1, tcpConfig1)
                            .instance()
                            .name("server")
                            .create();
                    closeables.add(map2);

                }
                {
                    final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                            (8087, new InetSocketAddress("localhost", 8086)).name("map2")
                            .heartBeatInterval(1, TimeUnit.SECONDS)
                            .tcpBufferSize(1024 * 64);

                    map1 = builder
                            .replication((byte) 2, tcpConfig2)
                            .instance()
                            .name("map2")
                            .create();
                    closeables.add(map1);
                    return map1;

                }


            }


            case STATELESS: {
                {
                    final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig
                            .of(8086).name("server")
                            .heartBeatInterval(1, TimeUnit.SECONDS)
                            .tcpBufferSize(1024 * 64);


                    map2 = builder
                            .replication((byte) 1, tcpConfig1)
                            .instance()
                            .name("server")
                            .create();
                    closeables.add(map2);

                }
                {
                    map1 = localClient(8086);

                    closeables.add(map1);
                    return map1;

                }
            }

            default:
                throw new IllegalStateException();
        }
    }

    static class PrefixStringFunction implements Function<String, String> {
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


    @Test
    public void testCharArrayValue() throws ExecutionException, InterruptedException, IOException {

        int valueSize = 10;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);

            assertEquals(Chars.asList(expected), Chars.asList(map.get("Key")));
            mapChecks();
        }
    }


    @Test
    public void testByteArrayArrayValue() throws ExecutionException, InterruptedException, IOException {

        int valueSize = 10;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<byte[], byte[][]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[][].class);

        try (ChronicleMap<byte[], byte[][]> map = newInstance(builder)) {
            byte[] bytes1 = "value1".getBytes();
            byte[] bytes2 = "value2".getBytes();
            byte[][] value = {bytes1,bytes2};
            map.put("Key".getBytes(), value);

            assertEquals(value, map.get("Key".getBytes()));
            mapChecks();
        }
    }

    @Test
    public void bondExample() throws IOException, InterruptedException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // acquireContext not supported by the STATELESS client

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(String.class, BondVOInterface.class)
                .averageKeySize(10);

        try (ChronicleMap<String, BondVOInterface> chm = newInstance(builder)) {
            BondVOInterface bondVO = chm.newValueInstance();
            try (MapKeyContext wc = chm.acquireContext("369604103", bondVO)) {
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
    public void testLargeCharSequenceValueWriteOnly() throws ExecutionException, InterruptedException, IOException {

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
            throws ExecutionException, InterruptedException, IOException {

        int salefactor = 100;
        int valueSize = 10 * salefactor;

        char[] expected = new char[valueSize];
        Arrays.fill(expected, 'X');

        ChronicleMapBuilder<CharSequence, char[]> builder = ChronicleMapBuilder
                .of(CharSequence.class, char[].class)
                .averageKeySize(10)
                .averageValueSize(10);

        try (ChronicleMap<CharSequence, char[]> map = newInstance(builder)) {
            map.put("Key", expected);
            mapChecks();
        }
    }


    @Test
    public void testKeyValueSizeBySample() throws ExecutionException, InterruptedException,
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
            throws ExecutionException, InterruptedException, IOException {

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
    public void testStringStringMap() throws ExecutionException, InterruptedException,
            IOException {


        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            assertEquals("World", map.get("Hello"));

            assertEquals("New World", map.getMapped("Hello", new PrefixStringFunction("New ")));
            assertEquals(null, map.getMapped("No key", new PrefixStringFunction("New ")));
            mapChecks();
        }
    }


    private static class StringPrefixUnaryOperator implements UnaryOperator<String>, Serializable {

        private String prefix;

        StringPrefixUnaryOperator(final String prefix1) {
            prefix = prefix1;
        }

        @Override
        public String update(String s) {
            return prefix + s;
        }

    }


    @Test
    public void testStringStringMapMutableValue() throws ExecutionException, InterruptedException, IOException {

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class);

        try (ChronicleMap<String, String> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.putMapped("Hello", new StringPrefixUnaryOperator("New "));
            mapChecks();
        }
    }

    @Test
    public void testCharSequenceMixingKeyTypes() throws ExecutionException, InterruptedException, IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            map.put("Hello", "World");
            map.put(new StringBuilder("Hello"), "World2");

            Assert.assertEquals("World2", map.get("Hello"));
            mapChecks();
        }
    }

    @Test
    public void testCharSequenceMixingValueTypes() throws ExecutionException, InterruptedException, IOException {

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Hello", "World");
            map.put("Hello2", new StringBuilder("World2"));

            Assert.assertEquals("World2", map.get("Hello2"));
            Assert.assertEquals("World", map.get("Hello"));
            mapChecks();
        }
    }


    /**
     * CharSequence is more efficient when object creation is avoided. The key can only be on heap
     * and variable length serialised.
     */
    @Test
    public void testCharSequenceCharSequenceMap() throws ExecutionException, InterruptedException, IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // Function supported by the STATELESS client


        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {
            map.put("Hello", "World");
            StringBuilder key = new StringBuilder();
            key.append("key-").append(1);

            StringBuilder value = new StringBuilder();
            value.append("value-").append(1);
            map.put(key, value);
            assertEquals("value-1", map.get("key-1"));

            assertEquals(value, map.getUsing(key, value));
            assertEquals("value-1", value.toString());
            map.remove("key-1");
            assertNull(map.getUsing(key, value));


            assertEquals("New World", map.getMapped("Hello", new
                    Function<CharSequence, CharSequence>() {
                        @Override
                        public CharSequence apply(CharSequence s) {
                            return "New " + s;
                        }
                    }));
            assertEquals(null, map.getMapped("No key", new
                    Function<CharSequence, CharSequence>() {
                        @Override
                        public CharSequence apply(CharSequence s) {
                            return "New " + s;
                        }
                    }));

            assertEquals("New World !!", map.putMapped("Hello", new
                    UnaryOperator<CharSequence>() {
                        @Override
                        public CharSequence update(CharSequence s) {
                            ((StringBuilder) s).append(" !!");
                            return "New " + s;
                        }
                    }));

            assertEquals("New World !!", map.get("Hello").toString());

            assertEquals("New !!", map.putMapped("no-key", new
                    UnaryOperator<CharSequence>() {
                        @Override
                        public CharSequence update(CharSequence s) {
                            ((StringBuilder) s).append("!!");
                            return "New " + s;
                        }
                    }));

            mapChecks();
        }
    }


    @Test
    public void testAcquireUsingWithCharSequence() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client


        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class);

        try (ChronicleMap<CharSequence, CharSequence> map = newInstance(builder)) {

            CharSequence using = map.newValueInstance();

            try (MapKeyContext wc = map.acquireContext("1", using)) {
                assertTrue(using instanceof StringBuilder);
                ((StringBuilder) using).append("Hello World");
            }

            assertEquals("Hello World", map.get("1"));
            mapChecks();
        }
    }

    @Test
    public void testGetUsingWithIntValueNoValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<CharSequence, IntValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, IntValue.class);

        try (ChronicleMap<CharSequence, IntValue> map = newInstance(builder)) {

            try (MapKeyContext<CharSequence, IntValue> c = map.context("1")) {
                IntValue value = c.get();
                assertNull(value);
            }

            assertEquals(null, map.get("1"));
            mapChecks();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireUsingImmutableUsing() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            throw new IllegalArgumentException(); // acquireContext supported by the STATELESS

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {


            IntValue using = map.newKeyInstance();
            using.setValue(1);

            try (MapKeyContext c = map.acquireContext(using, "")) {
                assertTrue(using instanceof IntValue);
                using.setValue(1);
            }

            assertEquals(null, map.get("1"));
            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithIntValueKeyStringBuilderValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // acquireContext supported by the STATELESS client

        ChronicleMapBuilder<IntValue, StringBuilder> builder = ChronicleMapBuilder
                .of(IntValue.class, StringBuilder.class);

        try (ChronicleMap<IntValue, StringBuilder> map = newInstance(builder)) {


            IntValue key = map.newKeyInstance();
            key.setValue(1);

            StringBuilder using = map.newValueInstance();

            try (MapKeyContext rc = map.acquireContext(key, using)) {
                using.append("Hello");
            }

            assertEquals("Hello", map.get(key).toString());
            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithIntValueKey() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {

            IntValue key = map.newKeyInstance();
            key.setValue(1);

            CharSequence using = map.newValueInstance();

            try (MapKeyContext rc = map.acquireContext(key, using)) {
                key.setValue(3);
                ((StringBuilder) using).append("Hello");
            }

            key.setValue(2);
            try (MapKeyContext rc = map.acquireContext(key, using)) {
                ((StringBuilder) using).append("World");
            }

            key.setValue(1);
            assertEquals("Hello", map.get(key));
            mapChecks();
        }
    }

    @Test
    public void testAcquireUsingWithByteBufferBytesValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return;

        ChronicleMapBuilder<IntValue, CharSequence> builder = ChronicleMapBuilder
                .of(IntValue.class, CharSequence.class);

        try (ChronicleMap<IntValue, CharSequence> map = newInstance(builder)) {

            IntValue key = map.newKeyInstance();
            key.setValue(1);

            ByteBufferBytes value = new ByteBufferBytes(ByteBuffer.allocate(10));
            value.limit(0);

            try (MapKeyContext rc = map.acquireContext(key, value)) {
                assertTrue(key instanceof IntValue);
                assertTrue(value instanceof CharSequence);
            }

            assertTrue(map.get(key).length() == 0);
            mapChecks();
        }
    }


    /**
     * StringValue represents any bean which contains a String Value
     */
    @Test
    public void testStringValueStringValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<StringValue, StringValue> builder = ChronicleMapBuilder
                .of(StringValue.class, StringValue.class);

        try (ChronicleMap<StringValue, StringValue> map = newInstance(builder)) {
            StringValue key1 = map.newKeyInstance();
            StringValue key2 = map.newKeyInstance();
            StringValue value1 = map.newValueInstance();
            StringValue value2 = map.newValueInstance();

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
            try (MapKeyContext<StringValue, StringValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                StringValue v = rc.get();
                assertEquals("11", v.getValue());
                v.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }

            mapChecks();

            try (MapKeyContext<StringValue, StringValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                StringValue v = rc.get();
                assertEquals("22", v.getValue());
                v.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }

            mapChecks();

            try (MapKeyContext<StringValue, StringValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                StringValue v = rc.get();
                assertEquals("11", v.getValue());
                v.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }

            mapChecks();

            try (MapKeyContext<StringValue, StringValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                StringValue v = rc.get();
                assertEquals("22", v.getValue());
                v.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }

            key1.setValue("3");
            try (MapKeyContext rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }

            key2.setValue("4");
            try (MapKeyContext rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext wc = map.acquireContext(key1, value1)) {
                assertEquals("", value1.getValue());
                value1.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value1.setValue(sb);
            }

            mapChecks();

            try (MapKeyContext wc = map.acquireContext(key1, value2)) {
                assertEquals("123", value2.getValue());
                value2.setValue(value2.getValue() + '4');
                assertEquals("1234", value2.getValue());
            }

            mapChecks();

            try (MapKeyContext<StringValue, StringValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals("1234", rc.get().getValue());
            }

            mapChecks();

            try (MapKeyContext wc = map.acquireContext(key2, value2)) {
                assertEquals("", value2.getValue());
                value2.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value2.setValue(sb);
            }

            mapChecks();

            try (MapKeyContext wc = map.acquireContext(key2, value1)) {
                assertEquals("123", value1.getValue());
                value1.setValue(value1.getValue() + '4');
                assertEquals("1234", value1.getValue());
            }

            mapChecks();

            try (MapKeyContext<StringValue, StringValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals("1234", rc.get().getValue());
            }

            mapChecks();
        }
    }

    @Test
    public void testIntegerIntegerMap()
            throws ExecutionException, InterruptedException, IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return; // Function supported by the STATELESS client

        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMapBuilder
                .of(Integer.class, Integer.class);

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

            assertEquals((Integer) 110, map.getMapped(1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            mapChecks();

            assertEquals(null, map.getMapped(-1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            mapChecks();

            try {
                map.putMapped(1, new UnaryOperator<Integer>() {
                    @Override
                    public Integer update(Integer s) {
                        return s + 1;
                    }
                });
            } catch (Exception todoMoreSpecificException) {
            }
            mapChecks();

        }
    }

    @Test
    public void testLongLongMap() throws ExecutionException, InterruptedException, IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return;


        ChronicleMapBuilder<Long, Long> builder = ChronicleMapBuilder
                .of(Long.class, Long.class);

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

            assertEquals((Long) 110L, map.getMapped(1L, new Function<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.getMapped(-1L, new Function<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }));

            mapChecks();

            try {
                map.putMapped(1L, new UnaryOperator<Long>() {
                    @Override
                    public Long update(Long s) {
                        return s + 1;
                    }
                });
            } catch (Exception todoMoreSpecificException) {
            }

            mapChecks();
        }
    }

    @Test
    public void testDoubleDoubleMap() throws ExecutionException, InterruptedException, IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return;


        ChronicleMapBuilder<Double, Double> builder = ChronicleMapBuilder
                .of(Double.class, Double.class);

        try (ChronicleMap<Double, Double> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            map.put(1.0, 11.0);
            assertEquals((Double) 11.0, map.get(1.0));

            map.put(2.0, 22.0);
            assertEquals((Double) 22.0, map.get(2.0));

            assertEquals(null, map.get(3.0));
            assertEquals(null, map.get(4.0));

            assertEquals((Double) 110.0, map.getMapped(1.0, new Function<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.getMapped(-1.0, new Function<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }));

            try {
map.putMapped(1.0, new UnaryOperator<Double>() {
    @Override
    public Double update(Double s) {
        return s + 1;
    }
});

            } catch (Exception todoMoreSpecificException) {
            }


        }
    }

    @Test
    public void testByteArrayByteArrayMap()
            throws ExecutionException, InterruptedException, IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return;

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
                    map.getMapped(key1, new Function<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            })));
            assertEquals(null, map.getMapped(key2, new Function<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            }));

            assertTrue(Arrays.equals(new byte[]{12, 10},
                    map.putMapped(key1, new UnaryOperator<byte[]>() {
                @Override
                public byte[] update(byte[] s) {
                    s[0]++;
                    s[1]--;
                    return Arrays.copyOf(s, 2);
                }
            })));

            byte[] a2 = map.get(key1);
            assertTrue(Arrays.equals(new byte[]{12, 10}, a2));


        }
    }


    // @Ignore("HCOLL-268 JSON serialisation issue")
    @Test
    public void testByteBufferByteBufferDefaultKeyValueMarshaller() throws ExecutionException,
            InterruptedException, IOException {

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
            throws ExecutionException, InterruptedException, IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

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

            final Function<ByteBuffer, ByteBuffer> function =
                    new Function<ByteBuffer, ByteBuffer>() {
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
                    map.putMapped(key1, new UnaryOperator<ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(1, (byte) (s.get(1) - 1));
                    return function.apply(s);
                }
            }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.get(key1));

            mapChecks();

            map.put(key1, value1);
            map.put(key2, value2);
            ByteBuffer valueA = ByteBuffer.allocateDirect(8);
            ByteBuffer valueB = ByteBuffer.allocate(8);
//            assertBBEquals(value1, valueA);
            try (MapKeyContext<ByteBuffer, ByteBuffer> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertBBEquals(value1, rc.getUsing(valueA));
            }
            try (MapKeyContext<ByteBuffer, ByteBuffer> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertBBEquals(value2, rc.getUsing(valueA));
            }

            try (MapKeyContext<ByteBuffer, ByteBuffer> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertBBEquals(value1, rc.getUsing(valueB));
            }
            try (MapKeyContext<ByteBuffer, ByteBuffer> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertBBEquals(value2, rc.getUsing(valueB));
            }

            try (MapKeyContext<ByteBuffer, ByteBuffer> wc = map.acquireContext(key1, valueA)) {
                assertBBEquals(value1, valueA);
                appendMode(valueA);
                valueA.clear();
                valueA.putInt(12345);
                valueA.flip();
            }


            value1.clear();
            value1.putInt(12345);
            value1.flip();


            try (MapKeyContext wc = map.acquireContext(key1, valueB)) {
                assertBBEquals(value1, valueB);
                appendMode(valueB);
                valueB.putShort((short) 12345);
                valueB.flip();
            }

            try (MapKeyContext<ByteBuffer, ByteBuffer> rc = map.context(key1)) {
                assertTrue(rc.containsKey());

                ByteBuffer bb1 = ByteBuffer.allocate(8);
                bb1.put(value1);
                bb1.putShort((short) 12345);
                bb1.flip();
                assertBBEquals(bb1, rc.getUsing(valueA));
            }

            mapChecks();
        }
    }

    private static void appendMode(ByteBuffer valueA) {
        valueA.position(valueA.limit());
        valueA.limit(valueA.capacity());
    }

    //@Ignore("HCOLL-268 JSON serialisation issue")
    @Test
    public void testByteBufferDirectByteBufferMap()
            throws ExecutionException, InterruptedException, IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; //  not supported by the STATELESS client

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
            final Function<ByteBuffer, ByteBuffer> function =
                    new Function<ByteBuffer, ByteBuffer>() {
                @Override
                public ByteBuffer apply(ByteBuffer s) {
                    ByteBuffer slice = s.slice();
                    slice.limit(2);
                    return slice;
                }
            };
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.getMapped(key1, function));
            assertEquals(null, map.getMapped(key2, function));
            mapChecks();
            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}),
                    map.putMapped(key1, new UnaryOperator<ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(1, (byte) (s.get(1) - 1));
                    return function.apply(s);
                }
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
        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, IntValue> builder = ChronicleMapBuilder
                .of(IntValue.class, IntValue.class);

        try (ChronicleMap<IntValue, IntValue> map = newInstance(builder)) {
            // this may change due to alignment
//            assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            IntValue value1 = map.newValueInstance();
            IntValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<IntValue, IntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO review -- the previous version of this block:
            // acquiring for value1, comparing value2 -- as intended?
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<IntValue, IntValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (MapKeyContext<IntValue, IntValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }

            try (MapKeyContext<IntValue, IntValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext<IntValue, IntValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (MapKeyContext<IntValue, IntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }
            mapChecks();

        }
    }

    @Test
    @Ignore("HCOLL-241 Generated code creates a field too large ie. it ignores the @Range")
    public void testUnsignedIntValueUnsignedIntValueMapEntrySize() throws IOException {

        // TODO once this is working, merge the next test.
        ChronicleMapBuilder<UnsignedIntValue, UnsignedIntValue> builder = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class);

        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = newInstance(builder)) {

            // this may change due to alignment
            //assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            UnsignedIntValue key1 = map.newKeyInstance();
            UnsignedIntValue value1 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));
            mapChecks();
        }
    }


    /**
     * For unsigned int -> unsigned int entries, the key can be on heap or off heap.
     */
    @Test
    public void testUnsignedIntValueUnsignedIntValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<UnsignedIntValue, UnsignedIntValue> builder = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class);

        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = newInstance(builder)) {


            UnsignedIntValue key1 = map.newKeyInstance();
            UnsignedIntValue key2 = map.newKeyInstance();
            UnsignedIntValue value1 = map.newValueInstance();
            UnsignedIntValue value2 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO review suspicious block
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (MapKeyContext wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }

            try (MapKeyContext wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (MapKeyContext<UnsignedIntValue, UnsignedIntValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueShortValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, ShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, ShortValue.class);

        try (ChronicleMap<IntValue, ShortValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(6, entrySize(map));


            //     assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            ShortValue value1 = map.newValueInstance();
            ShortValue value2 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue((short) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue((short) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, ShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO the same as above.
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<?, ShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<?, ShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<?, ShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, ShortValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, ShortValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, ShortValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue((short) 123);
                assertEquals(123, value1.getValue());
            }
            try (MapKeyContext<?, ShortValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue((short) (1230 - 123));
                assertEquals(1230, value2.getValue());
            }
            try (MapKeyContext<?, ShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }

            try (MapKeyContext<?, ShortValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((short) 123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext<?, ShortValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((short) (1230 - 123));
                assertEquals(1230, value1.getValue());
            }
            try (MapKeyContext<?, ShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int -> unsigned short values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedShortValueMap() throws IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return;

        ChronicleMapBuilder<IntValue, UnsignedShortValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedShortValue.class);

        try (ChronicleMap<IntValue, UnsignedShortValue> map = newInstance(builder)) {


            // this may change due to alignment
            // assertEquals(8, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            UnsignedShortValue value1 = map.newValueInstance();
            UnsignedShortValue value2 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO the same as above.
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, UnsignedShortValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }

            try (MapKeyContext<?, UnsignedShortValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (MapKeyContext<?, UnsignedShortValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueCharValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, CharValue> builder = ChronicleMapBuilder
                .of(IntValue.class, CharValue.class);

        try (ChronicleMap<IntValue, CharValue> map = newInstance(builder)) {


            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);
            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            CharValue value1 = map.newValueInstance();
            CharValue value2 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue((char) 11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue((char) 22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, CharValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO The same as above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<?, CharValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<?, CharValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<?, CharValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, CharValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, CharValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, CharValue> wc = map.acquireContext(key1, value1)) {
                assertEquals('\0', value1.getValue());
                value1.setValue('@');
                assertEquals('@', value1.getValue());
            }
            try (MapKeyContext wc = map.acquireContext(key1, value2)) {
                assertEquals('@', value2.getValue());
                value2.setValue('#');
                assertEquals('#', value2.getValue());
            }
            try (MapKeyContext<?, CharValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals('#', rc.get().getValue());
            }

            try (MapKeyContext<?, CharValue> wc = map.acquireContext(key2, value2)) {
                assertEquals('\0', value2.getValue());
                value2.setValue(';');
                assertEquals(';', value2.getValue());
            }
            try (MapKeyContext<?, CharValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(';', value1.getValue());
                value1.setValue('[');
                assertEquals('[', value1.getValue());
            }
            try (MapKeyContext<?, CharValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals('[', rc.get().getValue());
            }
        }
    }

    /**
     * For int-> byte entries, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedByteMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, UnsignedByteValue> builder = ChronicleMapBuilder
                .of(IntValue.class, UnsignedByteValue.class);

        try (ChronicleMap<IntValue, UnsignedByteValue> map = newInstance(builder)) {


            // TODO should be 5, but shorter fields based on range doesn't seem to be implemented
            // on data value generation level yet
            //assertEquals(8, entrySize(map)); this may change due to alignmented
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            UnsignedByteValue value1 = map.newValueInstance();
            UnsignedByteValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO the same as above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, UnsignedByteValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(234);
                assertEquals(234, value1.getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(234, value2.getValue());
                value2.addValue(-100);
                assertEquals(134, value2.getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(134, rc.get().getValue());
            }

            try (MapKeyContext<?, UnsignedByteValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((byte) 123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((byte) -111);
                assertEquals(12, value1.getValue());
            }
            try (MapKeyContext<?, UnsignedByteValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(12, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueBooleanValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<IntValue, BooleanValue> builder = ChronicleMapBuilder
                .of(IntValue.class, BooleanValue.class);

        try (ChronicleMap<IntValue, BooleanValue> map = newInstance(builder)) {


            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            IntValue key1 = map.newKeyInstance();
            IntValue key2 = map.newKeyInstance();
            BooleanValue value1 = map.newValueInstance();
            BooleanValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(true);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(false);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, BooleanValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(true, rc.get().getValue());
            }
            // TODO the same as above. copy paste, copy paste, copy-paste...
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(false, value2.getValue());
//            }
            try (MapKeyContext<?, BooleanValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(false, rc.get().getValue());
            }
            try (MapKeyContext<?, BooleanValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(true, rc.get().getValue());
            }
            try (MapKeyContext<?, BooleanValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(false, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, BooleanValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, BooleanValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, BooleanValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(false, value1.getValue());
                value1.setValue(true);
                assertEquals(true, value1.getValue());
            }
            try (MapKeyContext<?, BooleanValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(true, value2.getValue());
                value2.setValue(false);
                assertEquals(false, value2.getValue());
            }
            try (MapKeyContext<?, BooleanValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(false, rc.get().getValue());
            }

            try (MapKeyContext<?, BooleanValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(false, value2.getValue());
                value2.setValue(true);
                assertEquals(true, value2.getValue());
            }
            try (MapKeyContext<?, BooleanValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(true, value1.getValue());
                value1.setValue(false);
                assertEquals(false, value1.getValue());
            }
            try (MapKeyContext<?, BooleanValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(false, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For float values, the key can be on heap or off heap.
     */
    @Test
    public void testFloatValueFloatValueMap() throws IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<FloatValue, FloatValue> builder = ChronicleMapBuilder
                .of(FloatValue.class, FloatValue.class);

        try (ChronicleMap<FloatValue, FloatValue> map = newInstance(builder)) {

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            FloatValue key1 = map.newKeyInstance();
            FloatValue key2 = map.newKeyInstance();
            FloatValue value1 = map.newValueInstance();
            FloatValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, FloatValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue(), 0);
            }
            // TODO see above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue(), 0);
//            }
            try (MapKeyContext<?, FloatValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue(), 0);
            }
            key1.setValue(3);
            try (MapKeyContext<?, FloatValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, FloatValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, FloatValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue(), 0);
            }

            try (MapKeyContext<?, FloatValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (MapKeyContext<?, FloatValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue(), 0);
            }
            mapChecks();
        }
    }


    /**
     * For double values, the key can be on heap or off heap.
     */
    @Test
    public void testDoubleValueDoubleValueMap() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<DoubleValue, DoubleValue> builder = ChronicleMapBuilder
                .of(DoubleValue.class, DoubleValue.class);

        try (ChronicleMap<DoubleValue, DoubleValue> map = newInstance(builder)) {

            // this may change due to alignment
            //assertEquals(16, entrySize(map));

            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            DoubleValue key1 = map.newKeyInstance();
            DoubleValue key2 = map.newKeyInstance();
            DoubleValue value1 = map.newValueInstance();
            DoubleValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(11);
            assertEquals(null, map.get(key1));

            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, DoubleValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue(), 0);
            }
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue(), 0);
//            }
            try (MapKeyContext<?, DoubleValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue(), 0);
            }
            key1.setValue(3);
            try (MapKeyContext<?, DoubleValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, DoubleValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, DoubleValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue(), 0);
            }

            try (MapKeyContext<?, DoubleValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (MapKeyContext<?, DoubleValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue(), 0);
            }
            mapChecks();
        }
    }

    /**
     * For long values, the key can be on heap or off heap.
     */
    @Test
    public void testLongValueLongValueMap() throws IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return;

        ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMapBuilder
                .of(LongValue.class, LongValue.class);

        try (ChronicleMap<LongValue, LongValue> map = newInstance(builder)) {

            // this may change due to alignment
            // assertEquals(16, entrySize(map));
            assertEquals(1, ((VanillaChronicleMap) map).maxChunksPerEntry);

            LongValue key1 = map.newKeyInstance();
            LongValue key2 = map.newKeyInstance();
            LongValue value1 = map.newValueInstance();
            LongValue value2 = map.newValueInstance();


            key1.setValue(1);
            value1.setValue(11);
            assertEquals(null, map.get(key1));
            map.put(key1, value1);

            key2.setValue(2);
            value2.setValue(22);
            map.put(key2, value2);
            assertEquals(value2, map.get(key2));

            try (MapKeyContext<?, LongValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            // TODO see above
//            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
//                assertTrue(rc.present());
//                assertEquals(22, value2.getValue());
//            }
            try (MapKeyContext<?, LongValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            try (MapKeyContext<?, LongValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(11, rc.get().getValue());
            }
            try (MapKeyContext<?, LongValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(22, rc.get().getValue());
            }
            key1.setValue(3);
            try (MapKeyContext<?, LongValue> rc = map.context(key1)) {
                assertFalse(rc.containsKey());
            }
            key2.setValue(4);
            try (MapKeyContext<?, LongValue> rc = map.context(key2)) {
                assertFalse(rc.containsKey());
            }

            try (MapKeyContext<?, LongValue> wc = map.acquireContext(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (MapKeyContext<?, LongValue> wc = map.acquireContext(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (MapKeyContext<?, LongValue> rc = map.context(key1)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }

            try (MapKeyContext<?, LongValue> wc = map.acquireContext(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (MapKeyContext<?, LongValue> wc = map.acquireContext(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (MapKeyContext<?, LongValue> rc = map.context(key2)) {
                assertTrue(rc.containsKey());
                assertEquals(1230, rc.get().getValue());
            }
            mapChecks();
        }
    }

    /**
     * For beans, the key can be on heap or off heap as long as the bean is not variable length.
     */
    @Test
    public void testBeanBeanMap() {
    }

    @Test
    public void testListValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<String, List<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<List<String>>) (Class) List.class)
                .valueMarshaller(ListMarshaller.of(new StringMarshaller(8)));


        try (ChronicleMap<String, List<String>> map = newInstance(builder)) {
            map.put("1", Collections.<String>emptyList());
            map.put("2", asList("two-A"));

            List<String> list1 = new ArrayList<>();
            try (MapKeyContext wc = map.acquireContext("1", list1)) {
                list1.add("one");
                assertEquals(asList("one"), list1);
            }
            List<String> list2 = new ArrayList<>();
            try (MapKeyContext rc = map.context("1")) {
                assertTrue(rc.containsKey());
                assertEquals(asList("one"), rc.getUsing(list2));
            }

            try (MapKeyContext rc = map.context("2")) {
                assertTrue(rc.containsKey());
                rc.getUsing(list2);
                list2.add("two-B");     // this is not written as it only a read context
                assertEquals(asList("two-A", "two-B"), list2);
            }

            try (MapKeyContext wc = map.acquireContext("2", list1)) {
                list1.add("two-C");
                assertEquals(asList("two-A", "two-C"), list1);
            }

            try (MapKeyContext rc = map.context("2")) {
                assertTrue(rc.containsKey());
                assertEquals(asList("two-A", "two-C"), rc.getUsing(list2));
            }
        }
    }

    @Test
    public void testSetValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<String, Set<String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Set<String>>) (Class) Set.class)
                .valueMarshaller(SetMarshaller.of(new StringMarshaller(8)));


        try (ChronicleMap<String, Set<String>> map = newInstance(builder)) {
            map.put("1", Collections.<String>emptySet());
            map.put("2", new LinkedHashSet<String>(asList("one")));

            Set<String> list1 = new LinkedHashSet<>();
            try (MapKeyContext wc = map.acquireContext("1", list1)) {
                list1.add("two");
                assertEquals(new LinkedHashSet<String>(asList("two")), list1);
            }
            Set<String> list2 = new LinkedHashSet<>();
            try (MapKeyContext<?, Set<String>> rc = map.context("1")) {
                assertTrue(rc.containsKey());
                assertEquals(new LinkedHashSet<String>(asList("two")), rc.getUsing(list2));
            }
            try (MapKeyContext wc = map.acquireContext("2", list1)) {
                list1.add("three");
                assertEquals(new LinkedHashSet<String>(asList("one", "three")), list1);
            }
            try (MapKeyContext<?, Set<String>> rc = map.context("2")) {
                assertTrue(rc.containsKey());
                assertEquals(new LinkedHashSet<String>(asList("one", "three")), rc.getUsing(list2));
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

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context supported by the STATELESS client

        ChronicleMapBuilder<String, Map<String, String>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, String>>) (Class) Map.class)
                .valueMarshaller(MapMarshaller.of(new StringMarshaller(16), new StringMarshaller
                        (16)));


        try (ChronicleMap<String, Map<String, String>> map = newInstance(builder)) {
            map.put("1", Collections.<String, String>emptyMap());
            map.put("2", mapOf("one", "uni"));

            Map<String, String> map1 = new LinkedHashMap<>();
            try (MapKeyContext wc = map.acquireContext("1", map1)) {
                map1.put("two", "bi");
                assertEquals(mapOf("two", "bi"), map1);
            }
            Map<String, String> map2 = new LinkedHashMap<>();
            try (MapKeyContext<?, Map<String, String>> rc = map.context("1")) {
                assertTrue(rc.containsKey());
                assertEquals(mapOf("two", "bi"), rc.getUsing(map2));
            }
            try (MapKeyContext wc = map.acquireContext("2", map1)) {
                map1.put("three", "tri");
                assertEquals(mapOf("one", "uni", "three", "tri"), map1);
            }
            try (MapKeyContext<?, Map<String, String>> rc = map.context("2")) {
                assertTrue(rc.containsKey());
                assertEquals(mapOf("one", "uni", "three", "tri"), rc.getUsing(map2));
            }
            mapChecks();
        }
    }

    @Test
    public void testMapStringIntegerValue() throws IOException {


        if (typeOfMap == TypeOfMap.STATELESS)
            return; // context not supported by the STATELESS client

        ChronicleMapBuilder<String, Map<String, Integer>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class)
                .valueMarshaller(MapMarshaller.of(new StringMarshaller(16), new
                        GenericEnumMarshaller<Integer>(Integer.class, 16)));

        try (ChronicleMap<String, Map<String, Integer>> map = newInstance(builder)) {
            map.put("1", Collections.<String, Integer>emptyMap());
            map.put("2", mapOf("one", 1));

            Map<String, Integer> map1 = new LinkedHashMap<>();
            try (MapKeyContext wc = map.acquireContext("1", map1)) {
                map1.put("two", 2);
                assertEquals(mapOf("two", 2), map1);
            }
            Map<String, Integer> map2 = new LinkedHashMap<>();
            try (MapKeyContext<?, Map<String, Integer>> rc = map.context("1")) {
                assertTrue(rc.containsKey());
                assertEquals(mapOf("two", 2), rc.getUsing(map2));
            }
            try (MapKeyContext wc = map.acquireContext("2", map1)) {
                map1.put("three", 3);
                assertEquals(mapOf("one", 1, "three", 3), map1);
            }
            try (MapKeyContext<?, Map<String, Integer>> rc = map.context("2")) {
                assertTrue(rc.containsKey());
                assertEquals(mapOf("one", 1, "three", 3), rc.getUsing(map2));
            }
            mapChecks();
        }
    }

    @Test
    public void testMapStringIntegerValueWithoutListMarshallers() throws IOException {
        ChronicleMapBuilder<String, Map<String, Integer>> builder = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class);
        try (ChronicleMap<String, Map<String, Integer>> map = newInstance(builder)) {
            map.put("1", Collections.<String, Integer>emptyMap());
            map.put("2", mapOf("two", 2));

            assertEquals(mapOf("two", 2), map.get("2"));
            mapChecks();
        }
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

    @Test
    public void testGeneratedDataValue() throws IOException {

        if (typeOfMap == TypeOfMap.STATELESS)
            return; // acquireContext not supported by the STATELESS client

        ChronicleMapBuilder<String, IBean> builder = ChronicleMapBuilder
                .of(String.class, IBean.class).averageKeySize(5).entries(1000);
        try (ChronicleMap<String, IBean> map = newInstance(builder)) {

            IBean iBean = map.newValueInstance();
            try (MapKeyContext cx = map.acquireContext("1",
                    iBean)) {
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


}

enum ToString implements Function<Object, String> {
    INSTANCE;

    @Override
    public String apply(Object o) {
        return String.valueOf(o);
    }

}

interface IBean {
    long getLong();

    void setLong(long num);

    double getDouble();

    void setDouble(double d);

    int getInt();

    void setInt(int i);

    void setInnerAt(@MaxSize(7) int index, Inner inner);

    Inner getInnerAt(int index);

    /* nested interface - empowering an Off-Heap hierarchical “TIER of prices”
    as array[ ] value */
    interface Inner {

        String getMessage();

        void setMessage(@MaxSize(20) String px);

    }
}

