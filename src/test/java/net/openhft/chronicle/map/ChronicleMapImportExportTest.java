package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.lang.values.LongValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class ChronicleMapImportExportTest {

    public static final String TMP = System.getProperty("java.io.tmpdir");

    @Test
    public void test() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        try (ChronicleMap<String, String> expected = ChronicleMapBuilder.of(String.class, String
                .class)
                .create()) {
            expected.put("hello", "world");
            expected.put("aKey", "aValue");
            expected.getAll(file);

            try (ChronicleMap<String, String> actual = ChronicleMapBuilder.of(String.class, String
                    .class)
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithMapValue() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsoluteFile());
        try (ChronicleMap<String, Map> expected = ChronicleMapBuilder.of(String.class, Map
                .class)
                .create()) {
            HashMap<String, String> data = new HashMap<>();
            data.put("myKey", "myValue");
            expected.put("hello", data);
            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = ChronicleMapBuilder.of(String.class, Map
                    .class)
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithMapOfMapValue() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsoluteFile());
        try (ChronicleMap expected = ChronicleMapBuilder.of
                (String.class, Map.class).create()) {
            HashMap<String, Map> data = new HashMap<>();
            HashMap<String, String> data2 = new HashMap<>();
            data2.put("nested", "map");
            data.put("myKey", data2);
            expected.put("hello", data);

            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = ChronicleMapBuilder.of(String.class, Map
                    .class)
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithIntegerAndDouble() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        try (ChronicleMap<Integer, Double> expected = ChronicleMapBuilder.of(Integer.class, Double
                .class)
                .create()) {
            expected.put(1, 1.0);

            expected.getAll(file);

            try (ChronicleMap<Integer, Double> actual = ChronicleMapBuilder.of(Integer.class, Double
                    .class)
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithCharSeq() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        try (ChronicleMap<CharSequence, CharSequence> expected = ChronicleMapBuilder.of(CharSequence.class, CharSequence
                .class)
                .create()) {
            expected.put("hello", "world");

            expected.getAll(file);

            try (ChronicleMap<CharSequence, CharSequence> actual = ChronicleMapBuilder.of(CharSequence.class, CharSequence
                    .class)
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }


    @Ignore("HCOLL-239 - The JSON to Map import/export should work on the stateless client.")
    @Test
    public void testToJsonWithStatlessClient() throws IOException, InterruptedException {
        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();
        try (ChronicleMap<CharSequence, CharSequence> expected = ChronicleMapBuilder.of(CharSequence.class, CharSequence
                .class)
                .create()) {
            try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
                try (ChronicleMap<Integer, CharSequence> actual = ChronicleMapBuilder.of(Integer
                        .class, CharSequence.class)
                        .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                    expected.put("hello", "world");

                    expected.getAll(file);

                    actual.putAll(file);

                    Assert.assertEquals(expected, actual);

                }
            }
        }
    }


    @Test
    public void testWithLongValue() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        try (ChronicleMap<CharSequence, LongValue> expected = ChronicleMapBuilder.of(CharSequence.class, LongValue
                .class)
                .create()) {
            LongValue value = expected.newValueInstance();

            // this will add the entry
            try (WriteContext<?, LongValue> context = expected.acquireUsingLocked("one", value)) {
                assertEquals(0, context.value().getValue());
                assert value == context.value();
                LongValue value1 = context.value();
                value1.addValue(1);
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, LongValue> actual = ChronicleMapBuilder.of(CharSequence.class, LongValue
                    .class)
                    .create()) {

                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }

    @Ignore("HCOLL-259 JSON<->CHM to support $$Native like BondVOInterface")
    @Test
    public void testBondVOInterface() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        try (ChronicleMap<CharSequence, BondVOInterface> expected = ChronicleMapBuilder.of(CharSequence.class, BondVOInterface
                .class)
                .create()) {
            BondVOInterface value = expected.newValueInstance();

            // this will add the entry
            try (WriteContext context = expected.acquireUsingLocked("one", value)) {
                value.setCoupon(8.98);
                assert value == context.value();

            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, LongValue> actual = ChronicleMapBuilder.of(CharSequence.class, LongValue
                    .class)
                    .create()) {

                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }
}
