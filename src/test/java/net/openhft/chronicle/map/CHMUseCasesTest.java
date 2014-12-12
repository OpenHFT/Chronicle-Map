package net.openhft.chronicle.map;

import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.values.*;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * This test enumerates common usecases for keys and values.
 */
// TODO Test for persisted map.
// TODO Test for stateless map.

public class CHMUseCasesTest {
    /**
     * String is not as efficient as CharSequence as a key or value but easier to use The key can
     * only be on heap and variable length serialised.
     */
    @Test
    public void testStringStringMap() throws ExecutionException, InterruptedException {
/* TODO run the same test for multiple types of map stores
        for(ChronicleMapBuilder<String, String> chmb : Arrays.asList(
                ChronicleMapBuilder.of(String.class, String.class),
                ChronicleMapBuilder.of(String.class, String.class).file(TMP_FILE),
                ChronicleMapBuilder.of(String.class, String.class).statelessClient(CONFIG)
        )) {
            try (ChronicleMap<String, String> map = chmb.create()) {
*/
        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class) // for testing purposes only
                .create()) {
            map.put("Hello", "World");
            assertEquals("World", map.get("Hello"));

            assertEquals("New World", map.getMapped("Hello", new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return "New " + s;
                }
            }));
            assertEquals(null, map.getMapped("No key", new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return "New " + s;
                }
            }));


        }
    }


    @Test
    public void testStringStringMapMutableValue() throws ExecutionException, InterruptedException {
/* TODO run the same test for multiple types of map stores
        for(ChronicleMapBuilder<String, String> chmb : Arrays.asList(
                ChronicleMapBuilder.of(String.class, String.class),
                ChronicleMapBuilder.of(String.class, String.class).file(TMP_FILE),
                ChronicleMapBuilder.of(String.class, String.class).statelessClient(CONFIG)
        )) {
            try (ChronicleMap<String, String> map = chmb.create()) {
*/
        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class) // for testing purposes only
                .create()) {
            map.put("Hello", "World");


            map.putMapped("Hello", new UnaryOperator<String>() {
                @Override
                public String update(String s) {
                    return "New " + s;
                }
            });
        }
    }

    /**
     * CharSequence is more efficient when object creation is avoided. The key can only be on heap
     * and variable length serialised.
     */
    @Test
    public void testCharSequenceCharSequenceMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class) // for testing purposes only
                .defaultValue("")
                .create()) {
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

        }
    }

    /**
     * StringValue represents any bean which contains a String Value
     */
    @Test
    public void testStringValueStringValueMap() {
        try (ChronicleMap<StringValue, StringValue> map = ChronicleMapBuilder
                .of(StringValue.class, StringValue.class) // for testing purposes only
                .create()) {
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

            StringBuilder sb = new StringBuilder();
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals("11", value1.getValue());
                value1.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals("22", value2.getValue());
                value2.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals("11", value2.getValue());
                value2.getUsingValue(sb);
                assertEquals("11", sb.toString());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals("22", value2.getValue());
                value2.getUsingValue(sb);
                assertEquals("22", sb.toString());
            }
            key1.setValue("3");
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue("4");
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals("", value1.getValue());
                value1.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value1.setValue(sb);
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals("123", value2.getValue());
                value2.setValue(value2.getValue() + '4');
                assertEquals("1234", value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals("1234", value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals("", value2.getValue());
                value2.getUsingValue(sb);
                assertEquals("", sb.toString());
                sb.append(123);
                value2.setValue(sb);
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals("123", value1.getValue());
                value1.setValue(value1.getValue() + '4');
                assertEquals("1234", value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals("1234", value2.getValue());
            }
        }
    }

    @Test
    public void testIntegerIntegerMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .create()) {
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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

            assertEquals((Integer) 110, map.getMapped(1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.getMapped(-1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            try {
                map.putMapped(1, new UnaryOperator<Integer>() {
                    @Override
                    public Integer update(Integer s) {
                        return s + 1;
                    }
                });
            } catch (Exception todoMoreSpecificException) {
            }


        }
    }

    @Test
    public void testLongLongMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<Long, Long> map = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .create()) {
            assertEquals(16, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
            map.put(1L, 11L);
            assertEquals((Long) 11L, map.get(1L));

            map.put(2L, 22L);
            assertEquals((Long) 22L, map.get(2L));

            assertEquals(null, map.get(3L));
            assertEquals(null, map.get(4L));

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

            try {
                map.putMapped(1L, new UnaryOperator<Long>() {
                    @Override
                    public Long update(Long s) {
                        return s + 1;
                    }
                });
            } catch (Exception todoMoreSpecificException) {
            }


        }
    }

    @Test
    public void testDoubleDoubleMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<Double, Double> map = ChronicleMapBuilder
                .of(Double.class, Double.class)
                .create()) {
            assertEquals(16, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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
    public void testByteArrayByteArrayMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .keySize(4).valueSize(4)
                .maxEntryOversizeFactor(1)
                .create()) {
            byte[] key1 = {1, 1, 1, 1};
            byte[] key2 = {2, 2, 2, 2};
            byte[] value1 = {11, 11, 11, 11};
            byte[] value2 = {22, 22, 22, 22};
            assertNull(map.put(key1, value1));
            assertTrue(Arrays.equals(value1, map.put(key1, value2)));
            assertTrue(Arrays.equals(value2, map.get(key1)));
            assertNull(map.get(key2));

            map.put(key1, value1);


            assertTrue(Arrays.equals(new byte[]{11, 11}, map.getMapped(key1, new Function<byte[], byte[]>() {
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

            assertTrue(Arrays.equals(new byte[]{12, 10}, map.putMapped(key1, new UnaryOperator<byte[]>() {
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

    @Test
    public void testByteBufferByteBufferMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<ByteBuffer, ByteBuffer> map = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .keyMarshaller(ByteBufferMarshaller.INSTANCE)
                .valueMarshaller(ByteBufferMarshaller.INSTANCE)
                .keySize(8)
                .valueSize(8)
                .maxEntryOversizeFactor(1)
                .create()) {
            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11});
            ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22});
            assertNull(map.put(key1, value1));
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value2, map.get(key1));
            assertNull(map.get(key2));

            final Function<ByteBuffer, ByteBuffer> function = new Function<ByteBuffer, ByteBuffer>() {
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

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.putMapped(key1, new UnaryOperator<ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(1, (byte) (s.get(1) - 1));
                    return function.apply(s);
                }
            }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.get(key1));

            map.put(key1, value1);
            map.put(key2, value2);
            ByteBuffer valueA = ByteBuffer.allocateDirect(8);
            ByteBuffer valueB = ByteBuffer.allocate(8);
//            assertBBEquals(value1, valueA);
            try (ReadContext<ByteBuffer, ByteBuffer> rc = map.getUsingLocked(key1, valueA)) {
                assertTrue(rc.present());
                assertBBEquals(value1, valueA);
            }
            try (ReadContext rc = map.getUsingLocked(key2, valueA)) {
                assertTrue(rc.present());
                assertBBEquals(value2, valueA);
            }

            try (ReadContext rc = map.getUsingLocked(key1, valueB)) {
                assertTrue(rc.present());
                assertBBEquals(value1, valueB);
            }
            try (ReadContext rc = map.getUsingLocked(key2, valueB)) {
                assertTrue(rc.present());
                assertBBEquals(value2, valueB);
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, valueA)) {
                assertBBEquals(value1, valueA);
                appendMode(valueA);
                valueA.clear();
                valueA.putInt(12345);
                valueA.flip();
            }


            value1.clear();
            value1.putInt(12345);
            value1.flip();


            try (WriteContext wc = map.acquireUsingLocked(key1, valueB)) {
                assertBBEquals(value1, valueB);
                appendMode(valueB);
                valueB.putShort((short) 12345);
                valueB.flip();
            }

            try (ReadContext rc = map.getUsingLocked(key1, valueA)) {
                assertTrue(rc.present());

                ByteBuffer bb1 = ByteBuffer.allocate(8);
                bb1.put(value1);
                bb1.putShort((short) 12345);
                bb1.flip();
                assertBBEquals(bb1, valueA);
            }


        }
    }

    private static void appendMode(ByteBuffer valueA) {
        valueA.position(valueA.limit());
        valueA.limit(valueA.capacity());
    }


    @Test
    public void testByteBufferDirectByteBufferMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<ByteBuffer, ByteBuffer> map = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .valueMarshaller(ByteBufferMarshaller.INSTANCE) // we should not have to to this !
                .keyMarshaller(ByteBufferMarshaller.INSTANCE)    // we should not have to to this !
                .keySize(5).valueSize(5)
                .maxEntryOversizeFactor(1)
                .create()) {
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

            final Function<ByteBuffer, ByteBuffer> function = new Function<ByteBuffer, ByteBuffer>() {
                @Override
                public ByteBuffer apply(ByteBuffer s) {
                    ByteBuffer slice = s.slice();
                    slice.limit(2);
                    return slice;
                }
            };
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.getMapped(key1, function));
            assertEquals(null, map.getMapped(key2, function));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.putMapped(key1, new UnaryOperator<ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(1, (byte) (s.get(1) - 1));
                    return function.apply(s);
                }
            }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.get(key1));

        }
    }

    private void assertBBEquals(ByteBuffer bb1, ByteBuffer bb2) {
        assertEquals(bb1.remaining(), bb2.remaining());
        for (int i = 0; i < bb1.remaining(); i++)
            assertEquals(bb1.get(bb1.position() + i), bb2.get(bb2.position() + i));
    }

    @Test
    public void testIntValueIntValueMap() {
        try (ChronicleMap<IntValue, IntValue> map = ChronicleMapBuilder
                .of(IntValue.class, IntValue.class)
                .create()) {
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue());
            }
        }
    }

    @Test
    @Ignore("Generated code creates a field too large ie. it ignores the @Range")
    public void testUnsignedIntValueUnsignedIntValueMapEntrySize() {
        // TODO once this is working, merge the next test.
        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class)
                .create()) {
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
            UnsignedIntValue key1 = map.newKeyInstance();
            UnsignedIntValue value1 = map.newValueInstance();

            key1.setValue(1);
            value1.setValue(11);
            map.put(key1, value1);
            assertEquals(value1, map.get(key1));

        }
    }

    /**
     * For unsigned int -> unsigned int entries, the key can be on heap or off heap.
     */
    @Test
    public void testUnsignedIntValueUnsignedIntValueMap() {
        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class)
                .create()) {


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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue());
            }
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueShortValueMap() {
        try (ChronicleMap<IntValue, ShortValue> map = ChronicleMapBuilder
                .of(IntValue.class, ShortValue.class)
                .create()) {
            assertEquals(6, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue((short) 123);
                assertEquals(123, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue((short) (1230 - 123));
                assertEquals(1230, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((short) 123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((short) (1230 - 123));
                assertEquals(1230, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue());
            }
        }
    }

    /**
     * For int -> unsigned short values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedShortValueMap() {
        try (ChronicleMap<IntValue, UnsignedShortValue> map = ChronicleMapBuilder
                .of(IntValue.class, UnsignedShortValue.class)
                .create()) {
            // TODO should be 6
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue());
            }
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueCharValueMap() {
        try (ChronicleMap<IntValue, CharValue> map = ChronicleMapBuilder
                .of(IntValue.class, CharValue.class)
                .create()) {
            assertEquals(6, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);
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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals('\0', value1.getValue());
                value1.setValue('@');
                assertEquals('@', value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals('@', value2.getValue());
                value2.setValue('#');
                assertEquals('#', value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals('#', value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals('\0', value2.getValue());
                value2.setValue(';');
                assertEquals(';', value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(';', value1.getValue());
                value1.setValue('[');
                assertEquals('[', value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals('[', value2.getValue());
            }
        }
    }

    /**
     * For int-> byte entries, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueUnsignedByteMap() {
        try (ChronicleMap<IntValue, UnsignedByteValue> map = ChronicleMapBuilder
                .of(IntValue.class, UnsignedByteValue.class)
                .create()) {
            // TODO should be 5, but shorter fields based on range doesn't seem to be implemented
            // on data value generation level yet
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);

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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(234);
                assertEquals(234, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(234, value2.getValue());
                value2.addValue(-100);
                assertEquals(134, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(134, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue((byte) 123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue((byte) -111);
                assertEquals(12, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(12, value2.getValue());
            }
        }
    }

    /**
     * For int values, the key can be on heap or off heap.
     */
    @Test
    public void testIntValueBooleanValueMap() {
        try (ChronicleMap<IntValue, BooleanValue> map = ChronicleMapBuilder
                .of(IntValue.class, BooleanValue.class)
                .create()) {
            assertEquals(5, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);

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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(true, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(false, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(true, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(false, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(false, value1.getValue());
                value1.setValue(true);
                assertEquals(true, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(true, value2.getValue());
                value2.setValue(false);
                assertEquals(false, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(false, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(false, value2.getValue());
                value2.setValue(true);
                assertEquals(true, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(true, value1.getValue());
                value1.setValue(false);
                assertEquals(false, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(false, value2.getValue());
            }
        }
    }

    /**
     * For float values, the key can be on heap or off heap.
     */
    @Test
    public void testFloatValueFloatValueMap() {
        try (ChronicleMap<FloatValue, FloatValue> map = ChronicleMapBuilder
                .of(FloatValue.class, FloatValue.class)
                .create()) {
            assertEquals(8, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);

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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue(), 0);
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue(), 0);
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue(), 0);
            }
        }
    }

    /**
     * For double values, the key can be on heap or off heap.
     */
    @Test
    public void testDoubleValueDoubleValueMap() {
        try (ChronicleMap<DoubleValue, DoubleValue> map = ChronicleMapBuilder
                .of(DoubleValue.class, DoubleValue.class)
                .create()) {
            assertEquals(16, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);

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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue(), 0);
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue(), 0);
                value1.addValue(123);
                assertEquals(123, value1.getValue(), 0);
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue(), 0);
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue(), 0);
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue(), 0);
                value2.addValue(123);
                assertEquals(123, value2.getValue(), 0);
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue(), 0);
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue(), 0);
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue(), 0);
            }
        }
    }

    /**
     * For long values, the key can be on heap or off heap.
     */
    @Test
    public void testLongValueLongValueMap() {
        try (ChronicleMap<LongValue, LongValue> map = ChronicleMapBuilder
                .of(LongValue.class, LongValue.class)
                .create()) {
            assertEquals(16, ((VanillaChronicleMap) map).entrySize);
            assertEquals(1, ((VanillaChronicleMap) map).maxEntryOversizeFactor);

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

            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(11, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value1)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value2)) {
                assertTrue(rc.present());
                assertEquals(11, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(22, value2.getValue());
            }
            key1.setValue(3);
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertFalse(rc.present());
            }
            key2.setValue(4);
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertFalse(rc.present());
            }

            try (WriteContext wc = map.acquireUsingLocked(key1, value1)) {
                assertEquals(0, value1.getValue());
                value1.addValue(123);
                assertEquals(123, value1.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key1, value2)) {
                assertEquals(123, value2.getValue());
                value2.addValue(1230 - 123);
                assertEquals(1230, value2.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key1, value1)) {
                assertTrue(rc.present());
                assertEquals(1230, value1.getValue());
            }

            try (WriteContext wc = map.acquireUsingLocked(key2, value2)) {
                assertEquals(0, value2.getValue());
                value2.addValue(123);
                assertEquals(123, value2.getValue());
            }
            try (WriteContext wc = map.acquireUsingLocked(key2, value1)) {
                assertEquals(123, value1.getValue());
                value1.addValue(1230 - 123);
                assertEquals(1230, value1.getValue());
            }
            try (ReadContext rc = map.getUsingLocked(key2, value2)) {
                assertTrue(rc.present());
                assertEquals(1230, value2.getValue());
            }
        }
    }

    /**
     * For beans, the key can be on heap or off heap as long as the bean is not variable length.
     */
    @Test
    public void testBeanBeanMap() {
    }

    @Test
    public void testListValue() {
        try (ChronicleMap<String, List<String>> map = ChronicleMapBuilder
                .of(String.class, (Class<List<String>>) (Class) List.class)
                .valueMarshaller(ListMarshaller.of(new StringMarshaller(8)))
                .create()) {
            map.put("1", Collections.<String>emptyList());
            map.put("2", Arrays.asList("two-A"));

            List<String> list1 = new ArrayList<>();
            try (WriteContext wc = map.acquireUsingLocked("1", list1)) {
                list1.add("one");
                assertEquals(Arrays.asList("one"), list1);
            }
            List<String> list2 = new ArrayList<>();
            try (ReadContext rc = map.getUsingLocked("1", list2)) {
                assertTrue(rc.present());
                assertEquals(Arrays.asList("one"), list2);
            }

            try (ReadContext rc = map.getUsingLocked("2", list2)) {
                assertTrue(rc.present());
                list2.add("two-B");     // this is not written as it only a read context
                assertEquals(Arrays.asList("two-A", "two-B"), list2);
            }

            try (WriteContext wc = map.acquireUsingLocked("2", list1)) {
                list1.add("two-C");
                assertEquals(Arrays.asList("two-A", "two-C"), list1);
            }

            try (ReadContext rc = map.getUsingLocked("2", list2)) {
                assertTrue(rc.present());
                assertEquals(Arrays.asList("two-A", "two-C"), list2);
            }
        }
    }

    @Test
    public void testSetValue() {
        try (ChronicleMap<String, Set<String>> map = ChronicleMapBuilder
                .of(String.class, (Class<Set<String>>) (Class) Set.class)
                .valueMarshaller(SetMarshaller.of(new StringMarshaller(8)))
                .create()) {
            map.put("1", Collections.<String>emptySet());
            map.put("2", new LinkedHashSet<String>(Arrays.asList("one")));

            Set<String> list1 = new LinkedHashSet<>();
            try (WriteContext wc = map.acquireUsingLocked("1", list1)) {
                list1.add("two");
                assertEquals(new LinkedHashSet<String>(Arrays.asList("two")), list1);
            }
            Set<String> list2 = new LinkedHashSet<>();
            try (ReadContext rc = map.getUsingLocked("1", list2)) {
                assertTrue(rc.present());
                assertEquals(new LinkedHashSet<String>(Arrays.asList("two")), list2);
            }
            try (WriteContext wc = map.acquireUsingLocked("2", list1)) {
                list1.add("three");
                assertEquals(new LinkedHashSet<String>(Arrays.asList("one", "three")), list1);
            }
            try (ReadContext rc = map.getUsingLocked("2", list2)) {
                assertTrue(rc.present());
                assertEquals(new LinkedHashSet<String>(Arrays.asList("one", "three")), list2);
            }
        }
    }

    @Test
    public void testMapStringStringValue() {
        try (ChronicleMap<String, Map<String, String>> map = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, String>>) (Class) Map.class)
                .valueMarshaller(MapMarshaller.of(new StringMarshaller(16), new StringMarshaller(16)))
                .create()) {
            map.put("1", Collections.<String, String>emptyMap());
            map.put("2", mapOf("one", "uni"));

            Map<String, String> map1 = new LinkedHashMap<>();
            try (WriteContext wc = map.acquireUsingLocked("1", map1)) {
                map1.put("two", "bi");
                assertEquals(mapOf("two", "bi"), map1);
            }
            Map<String, String> map2 = new LinkedHashMap<>();
            try (ReadContext rc = map.getUsingLocked("1", map2)) {
                assertTrue(rc.present());
                assertEquals(mapOf("two", "bi"), map2);
            }
            try (WriteContext wc = map.acquireUsingLocked("2", map1)) {
                map1.put("three", "tri");
                assertEquals(mapOf("one", "uni", "three", "tri"), map1);
            }
            try (ReadContext rc = map.getUsingLocked("2", map2)) {
                assertTrue(rc.present());
                assertEquals(mapOf("one", "uni", "three", "tri"), map2);
            }
        }
    }

    @Test
    public void testMapStringIntegerValue() {
        try (ChronicleMap<String, Map<String, Integer>> map = ChronicleMapBuilder
                .of(String.class, (Class<Map<String, Integer>>) (Class) Map.class)
                .valueMarshaller(MapMarshaller.of(new StringMarshaller(16), new GenericEnumMarshaller<Integer>(Integer.class, 16)))
                .create()) {
            map.put("1", Collections.<String, Integer>emptyMap());
            map.put("2", mapOf("one", 1));

            Map<String, Integer> map1 = new LinkedHashMap<>();
            try (WriteContext wc = map.acquireUsingLocked("1", map1)) {
                map1.put("two", 2);
                assertEquals(mapOf("two", 2), map1);
            }
            Map<String, Integer> map2 = new LinkedHashMap<>();
            try (ReadContext rc = map.getUsingLocked("1", map2)) {
                assertTrue(rc.present());
                assertEquals(mapOf("two", 2), map2);
            }
            try (WriteContext wc = map.acquireUsingLocked("2", map1)) {
                map1.put("three", 3);
                assertEquals(mapOf("one", 1, "three", 3), map1);
            }
            try (ReadContext rc = map.getUsingLocked("2", map2)) {
                assertTrue(rc.present());
                assertEquals(mapOf("one", 1, "three", 3), map2);
            }
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
}

enum ToString implements Function<Object, String> {
    INSTANCE;

    @Override
    public String apply(Object o) {
        return String.valueOf(o);
    }

}
/*
interface IBean {
    long getLong();

    void setLong(long num);

    double getDouble();

    void setDouble(double d);

    int getInt();

    void setInt(int i);
}
*/
