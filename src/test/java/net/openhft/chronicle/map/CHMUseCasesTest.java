package net.openhft.chronicle.map;

import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.DataValueClasses;
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
@Ignore
public class CHMUseCasesTest {
    /**
     * String is not as efficient as CharSequence as a key or value but easier to use The key can only be on heap and
     * variable length serialised.
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
                .of(String.class, String.class)
                .checkSerializedValues() // for testng purposes only
                .create()) {
            map.put("Hello", "World");
            assertEquals("World", map.get("Hello"));

            assertEquals("New World", map.mapForKey("Hello", new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return "New " + s;
                }
            }));
            assertEquals(null, map.mapForKey("No key", new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return "New " + s;
                }
            }));
            try {
                map.updateForKey("Hello", new Mutator<String, String>() {
                    @Override
                    public String update(String s) {
                        return "New " + s;
                    }
                });
                fail("Operation not supported as value is not mutable");
            } catch (Exception ignored) {
                // TODO replace with the specific exception.
            }

            assertEquals(null, map.putLater("Bye", "For now").get());
            assertEquals("For now", map.getLater("Bye").get());
            assertEquals("For now", map.removeLater("Bye").get());
            assertEquals(null, map.removeLater("Bye").get());
        }
    }

    /**
     * CharSequence is more efficient when object creation is avoided. The key can only be on heap and variable length
     * serialised.
     */
    @Test
    public void testCharSequenceCharSequenceMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .checkSerializedValues() // for testng purposes only
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
            map.close();

            assertEquals("New World", map.mapForKey("Hello", new Function<CharSequence, CharSequence>() {
                @Override
                public CharSequence apply(CharSequence s) {
                    return "New " + s;
                }
            }));
            assertEquals(null, map.mapForKey("No key", new Function<CharSequence, CharSequence>() {
                @Override
                public CharSequence apply(CharSequence s) {
                    return "New " + s;
                }
            }));

            assertEquals("New World !!", map.updateForKey("Hello", new Mutator<CharSequence, CharSequence>() {
                @Override
                public CharSequence update(CharSequence s) {
                    ((StringBuilder) s).append("!!");
                    return "New " + s;
                }
            }));

            assertEquals("New World !!", map.get("Hello").toString());

            assertEquals(null, map.updateForKey("no-key", new Mutator<CharSequence, CharSequence>() {
                @Override
                public CharSequence update(CharSequence s) {
                    ((StringBuilder) s).append("!!");
                    return "New " + s;
                }
            }));

            assertEquals(null, map.putLater("Bye", "For now").get());
            assertEquals("For now", map.getLater("Bye").get().toString());
            assertEquals("For now", map.removeLater("Bye").get().toString());
            assertEquals(null, map.removeLater("Bye").get());
        }
    }

    /**
     * StringValue represents any bean which contains a String Value
     */
    @Test
    public void testStringValueStringValueMap() {
        try (ChronicleMap<StringValue, StringValue> map = ChronicleMapBuilder
                .of(StringValue.class, StringValue.class)
                .checkSerializedValues() // for testng purposes only
                .create()) {

            StringValue key1 = DataValueClasses.newDirectInstance(StringValue.class);
            StringValue key2 = DataValueClasses.newInstance(StringValue.class);
            StringValue value1 = DataValueClasses.newDirectInstance(StringValue.class);
            StringValue value2 = DataValueClasses.newInstance(StringValue.class);

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
                .entrySize(8)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {
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

            assertEquals((Integer) 110, map.mapForKey(1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.mapForKey(-1, new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer s) {
                    return 10 * s;
                }
            }));

            try {
                map.updateForKey(1, new Mutator<Integer, Integer>() {
                    @Override
                    public Integer update(Integer s) {
                        return s + 1;
                    }
                });
                fail("Update of Integer not supported");
            } catch (Exception todoMoreSpecificException) {

            }

            assertEquals(null, map.putLater(3, 4).get());
            assertEquals((Integer) 4, map.getLater(3).get());
            assertEquals((Integer) 4, map.removeLater(3).get());
            assertEquals(null, map.removeLater(3).get());
        }
    }

    @Test
    public void testLongLongMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<Long, Long> map = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .entrySize(16)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {

            map.put(1L, 11L);
            assertEquals((Long) 11L, map.get(1L));

            map.put(2L, 22L);
            assertEquals((Long) 22L, map.get(2L));

            assertEquals(null, map.get(3L));
            assertEquals(null, map.get(4L));

            assertEquals((Long) 110L, map.mapForKey(1L, new Function<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.mapForKey(-1L, new Function<Long, Long>() {
                @Override
                public Long apply(Long s) {
                    return 10 * s;
                }
            }));

            try {
                map.updateForKey(1L, new Mutator<Long, Long>() {
                    @Override
                    public Long update(Long s) {
                        return s + 1;
                    }
                });
                fail("Update of Long not supported");
            } catch (Exception todoMoreSpecificException) {

            }

            assertEquals(null, map.putLater(3L, 4L).get());
            assertEquals((Long) 4L, map.getLater(3L).get());
            assertEquals((Long) 4L, map.removeLater(3L).get());
            assertEquals(null, map.removeLater(3L).get());
        }
    }

    @Test
    public void testDoubleDoubleMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<Double, Double> map = ChronicleMapBuilder
                .of(Double.class, Double.class)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(16)
                .create()) {

            map.put(1.0, 11.0);
            assertEquals((Double) 11.0, map.get(1.0));

            map.put(2.0, 22.0);
            assertEquals((Double) 22.0, map.get(2.0));

            assertEquals(null, map.get(3.0));
            assertEquals(null, map.get(4.0));

            assertEquals((Double) 110.0, map.mapForKey(1.0, new Function<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }));
            assertEquals(null, map.mapForKey(-1.0, new Function<Double, Double>() {
                @Override
                public Double apply(Double s) {
                    return 10 * s;
                }
            }));

            try {
                map.updateForKey(1.0, new Mutator<Double, Double>() {
                    @Override
                    public Double update(Double s) {
                        return s + 1;
                    }
                });
                fail("Update of Double not supported");
            } catch (Exception todoMoreSpecificException) {

            }

            assertEquals(null, map.putLater(3.0, 4.0).get());
            assertEquals((Double) 4.0, map.getLater(3.0).get());
            assertEquals((Double) 4.0, map.removeLater(3.0).get());
            assertEquals(null, map.removeLater(3.0).get());
        }
    }

    @Test
    public void testByteArrayByteArrayMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(12)
                .create()) {
            byte[] key1 = {1, 1, 1, 1};
            byte[] key2 = {2, 2, 2, 2};
            byte[] value1 = {11, 11, 11, 11};
            byte[] value2 = {22, 22, 22, 22};
            assertNull(map.put(key1, value1));
            assertTrue(Arrays.equals(value1, map.put(key1, value2)));
            assertTrue(Arrays.equals(value1, map.get(key1)));
            assertNull(map.get(key2));

            assertTrue(Arrays.equals(new byte[]{11, 11}, map.mapForKey(key1, new Function<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            })));
            assertEquals(null, map.mapForKey(key2, new Function<byte[], byte[]>() {
                @Override
                public byte[] apply(byte[] s) {
                    return Arrays.copyOf(s, 2);
                }
            }));

            assertTrue(Arrays.equals(new byte[]{12, 10}, map.updateForKey(key1, new Mutator<byte[], byte[]>() {
                @Override
                public byte[] update(byte[] s) {
                    s[0]++;
                    s[1]--;
                    return Arrays.copyOf(s, 2);
                }
            })));

            assertTrue(Arrays.equals(new byte[]{12, 10, 11, 11}, map.get(key1)));

            byte[] key3 = {3, 3, 3, 3};
            byte[] value3 = {4, 4, 4, 4};

            assertEquals(null, map.putLater(key3, value3).get());
            assertTrue(Arrays.equals(value3, map.getLater(key3).get()));
            assertTrue(Arrays.equals(value3, map.removeLater(key3).get()));
            assertEquals(null, map.removeLater(key3).get());
        }
    }

    @Test
    public void testByteBufferByteBufferMap() throws ExecutionException, InterruptedException {
        try (ChronicleMap<ByteBuffer, ByteBuffer> map = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(12)
                .create()) {
            ByteBuffer key1 = ByteBuffer.wrap(new byte[]{1, 1, 1, 1});
            ByteBuffer key2 = ByteBuffer.wrap(new byte[]{2, 2, 2, 2});
            ByteBuffer value1 = ByteBuffer.wrap(new byte[]{11, 11, 11, 11});
            ByteBuffer value2 = ByteBuffer.wrap(new byte[]{22, 22, 22, 22});
            assertNull(map.put(key1, value1));
            assertBBEquals(value1, map.put(key1, value2));
            assertBBEquals(value1, map.get(key1));
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
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.mapForKey(key1, function));
            assertEquals(null, map.mapForKey(key2, function));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.updateForKey(key1, new Mutator<ByteBuffer, ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(0, (byte) (s.get(0) - 1));
                    return function.apply(s);
                }
            }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10, 11, 11}), map.get(key1));


            map.put(key1, value1);
            map.put(key2, value2);
            ByteBuffer valueA = ByteBuffer.allocateDirect(8);
            ByteBuffer valueB = ByteBuffer.allocate(8);
            try (ReadContext rc = map.getUsingLocked(key1, valueA)) {
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
                valueA.putInt(12345);
                valueA.flip();
            }
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
                bb1.putInt(12345);
                bb1.flip();
                assertBBEquals(bb1, valueA);
            }

            ByteBuffer key3 = ByteBuffer.wrap(new byte[]{3, 3, 3, 3});
            ByteBuffer value3 = ByteBuffer.wrap(new byte[]{4, 4, 4, 4});

            assertEquals(null, map.putLater(key3, value3).get());
            assertBBEquals(value3, map.getLater(key3).get());
            assertBBEquals(value3, map.removeLater(key3).get());
            assertEquals(null, map.removeLater(key3).get());
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
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(12)
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
            assertBBEquals(value1, map.get(key1));
            assertNull(map.get(key2));

            final Function<ByteBuffer, ByteBuffer> function = new Function<ByteBuffer, ByteBuffer>() {
                @Override
                public ByteBuffer apply(ByteBuffer s) {
                    ByteBuffer slice = s.slice();
                    slice.limit(2);
                    return slice;
                }
            };
            assertBBEquals(ByteBuffer.wrap(new byte[]{11, 11}), map.mapForKey(key1, function));
            assertEquals(null, map.mapForKey(key2, function));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10}), map.updateForKey(key1, new Mutator<ByteBuffer, ByteBuffer>() {
                @Override
                public ByteBuffer update(ByteBuffer s) {
                    s.put(0, (byte) (s.get(0) + 1));
                    s.put(0, (byte) (s.get(0) - 1));
                    return function.apply(s);
                }
            }));

            assertBBEquals(ByteBuffer.wrap(new byte[]{12, 10, 11, 11}), map.get(key1));

            ByteBuffer key3 = ByteBuffer.wrap(new byte[]{3, 3, 3, 3});
            ByteBuffer value3 = ByteBuffer.allocateDirect(4);
            value3.put(new byte[]{4, 4, 4, 4});
            value3.flip();

            assertEquals(null, map.putLater(key3, value3).get());
            assertBBEquals(value3, map.getLater(key3).get());
            assertBBEquals(value3, map.removeLater(key3).get());
            assertEquals(null, map.removeLater(key3).get());
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
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(8)
                .create()) {
            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            IntValue value1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue value2 = DataValueClasses.newInstance(IntValue.class);

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
     * For unsigned int -> unsigned int entries, the key can be on heap or off heap.
     */
    @Test
    public void testUnsignedIntValueUnsignedIntValueMap() {
        try (ChronicleMap<UnsignedIntValue, UnsignedIntValue> map = ChronicleMapBuilder
                .of(UnsignedIntValue.class, UnsignedIntValue.class)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(8)
                .create()) {
            UnsignedIntValue key1 = DataValueClasses.newDirectInstance(UnsignedIntValue.class);
            UnsignedIntValue key2 = DataValueClasses.newInstance(UnsignedIntValue.class);
            UnsignedIntValue value1 = DataValueClasses.newDirectInstance(UnsignedIntValue.class);
            UnsignedIntValue value2 = DataValueClasses.newInstance(UnsignedIntValue.class);

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
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(6)
                .create()) {

            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            ShortValue value1 = DataValueClasses.newDirectInstance(ShortValue.class);
            ShortValue value2 = DataValueClasses.newInstance(ShortValue.class);

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
                .entrySize(6)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {
            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            UnsignedShortValue value1 = DataValueClasses.newDirectInstance(UnsignedShortValue.class);
            UnsignedShortValue value2 = DataValueClasses.newInstance(UnsignedShortValue.class);

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
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(6)
                .create()) {
            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            CharValue value1 = DataValueClasses.newDirectInstance(CharValue.class);
            CharValue value2 = DataValueClasses.newInstance(CharValue.class);

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
                .entrySize(5)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {
            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            UnsignedByteValue value1 = DataValueClasses.newDirectInstance(UnsignedByteValue.class);
            UnsignedByteValue value2 = DataValueClasses.newInstance(UnsignedByteValue.class);

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
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .entrySize(5)
                .create()) {
            IntValue key1 = DataValueClasses.newDirectInstance(IntValue.class);
            IntValue key2 = DataValueClasses.newInstance(IntValue.class);
            BooleanValue value1 = DataValueClasses.newDirectInstance(BooleanValue.class);
            BooleanValue value2 = DataValueClasses.newInstance(BooleanValue.class);

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
                .entrySize(8)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {
            FloatValue key1 = DataValueClasses.newDirectInstance(FloatValue.class);
            FloatValue key2 = DataValueClasses.newInstance(FloatValue.class);
            FloatValue value1 = DataValueClasses.newDirectInstance(FloatValue.class);
            FloatValue value2 = DataValueClasses.newInstance(FloatValue.class);

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
                .entrySize(16)
                .create()) {
            DoubleValue key1 = DataValueClasses.newDirectInstance(DoubleValue.class);
            DoubleValue key2 = DataValueClasses.newInstance(DoubleValue.class);
            DoubleValue value1 = DataValueClasses.newDirectInstance(DoubleValue.class);
            DoubleValue value2 = DataValueClasses.newInstance(DoubleValue.class);

            key1.setValue(1);
            value1.setValue(11);
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
                .entrySize(16)
                .disableOversizedEntries(true) // disabled for testing purposes only.
                .create()) {
            LongValue key1 = DataValueClasses.newDirectInstance(LongValue.class);
            LongValue key2 = DataValueClasses.newInstance(LongValue.class);
            LongValue value1 = DataValueClasses.newDirectInstance(LongValue.class);
            LongValue value2 = DataValueClasses.newInstance(LongValue.class);

            key1.setValue(1);
            value1.setValue(11);
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
            map.put("2", Arrays.asList("one"));

            List<String> list1 = new ArrayList<>();
            try (WriteContext wc = map.acquireUsingLocked("1", list1)) {
                list1.add("two");
                assertEquals(Arrays.asList("two"), list1);
            }
            List<String> list2 = new ArrayList<>();
            try (ReadContext rc = map.getUsingLocked("1", list2)) {
                assertTrue(rc.present());
                assertEquals(Arrays.asList("two"), list2);
            }
            try (WriteContext wc = map.acquireUsingLocked("2", list1)) {
                list1.add("three");
                assertEquals(Arrays.asList("three"), list1);
            }
            try (ReadContext rc = map.getUsingLocked("2", list2)) {
                assertTrue(rc.present());
                assertEquals(Arrays.asList("one", "three"), list2);
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
                assertEquals(new LinkedHashSet<String>(Arrays.asList("three")), list1);
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
