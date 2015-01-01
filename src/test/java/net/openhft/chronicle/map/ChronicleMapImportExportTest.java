package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.lang.values.LongValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static net.openhft.chronicle.map.StatelessClientTest.localClient;
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

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .entries(1000);
        try (ChronicleMap<String, String> expected = builder.create()) {
            expected.put("hello", "world");
            expected.put("aKey", "aValue");
            expected.getAll(file);

            try (ChronicleMap<String, String> actual = builder.create()) {
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
        ChronicleMapBuilder<String, Map> builder = ChronicleMapBuilder
                .of(String.class, Map.class)
                .averageKeySize("hello".length())
                .averageValueSize(100)
                .entries(1000);
        try (ChronicleMap<String, Map> expected = builder
                .create()) {
            HashMap<String, String> data = new HashMap<>();
            data.put("myKey", "myValue");
            expected.put("hello", data);
            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = builder.create()) {
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
        ChronicleMapBuilder<String, Map> builder = ChronicleMapBuilder
                .of(String.class, Map.class)
                .averageKeySize("hello".length()).averageValueSize(100).entries(1000);
        try (ChronicleMap expected = builder.create()) {
            HashMap<String, Map> data = new HashMap<>();
            HashMap<String, String> data2 = new HashMap<>();
            data2.put("nested", "map");
            data.put("myKey", data2);
            expected.put("hello", data);

            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = builder
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
        ChronicleMapBuilder<CharSequence, CharSequence> builder =
                ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .averageKeySize("hello".length()).averageValueSize("world".length())
                .entries(1000);
        try (ChronicleMap<CharSequence, CharSequence> expected = builder
                .create()) {
            expected.put("hello", "world");

            expected.getAll(file);

            try (ChronicleMap<CharSequence, CharSequence> actual = builder
                    .create()) {
                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            file.delete();
        }
    }


    @Test
    public void testFromHashMap() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        System.out.println(file.getCanonicalFile());

        File file2 = new File(TMP + "/chronicle-map-2" + System.nanoTime() + ".json");
        System.out.println(file2.getCanonicalFile());

        HashMap<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "one");
        map.put(2, "two");


        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);

        xstream.toXML(map, new FileOutputStream(file));


        try (ChronicleMap<Integer, String> expected = ChronicleMapBuilder
                .of(Integer.class, String.class)
                .averageValueSize(10)
                .entries(1000)
                .create()) {

            expected.put(1, "one");
            expected.put(2, "two");

            expected.getAll(file2);
            expected.putAll(file2);

            Assert.assertEquals(2, expected.size());
            Assert.assertEquals("one", expected.get(1));
            Assert.assertEquals("two", expected.get(2));
        }


        file.deleteOnExit();
    }


    @Test
    public void testToJsonWithStatelessClient() throws IOException, InterruptedException {
        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();
        File file2 = new File(TMP + "/chronicle-map-" + System.nanoTime() + "-2.json");
        file.deleteOnExit();
        try (ChronicleMap<CharSequence, CharSequence> expected =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class).create()) {
            try (ChronicleMap<CharSequence, CharSequence> serverMap = ChronicleMapBuilder
                    .of(CharSequence.class, CharSequence.class)
                    .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
                try (ChronicleMap<CharSequence, CharSequence> actual = localClient(8056)) {

                    expected.put("hello", "world");
                    expected.getAll(file);

                    actual.putAll(file);


                    Assert.assertEquals(expected, actual);

                    actual.getAll(file2);
                    actual.clear();
                    actual.putAll(file2);

                    Assert.assertEquals(expected, actual);

                }
            }
        }

        file.delete();
        file2.delete();
    }


    @Test
    public void testWithLongValue() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        //file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, LongValue.class)
                .averageKeySize("one".length())
                .entries(1000);
        try (ChronicleMap<CharSequence, LongValue> expected = builder.create()) {
            LongValue value = expected.newValueInstance();

            // this will add the entry
            try (WriteContext<?, LongValue> context = expected.acquireUsingLocked("one", value)) {
                assertEquals(0, context.value().getValue());
                assert value == context.value();
                LongValue value1 = context.value();
                value1.addValue(1);
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, LongValue> actual = builder.create()) {

                actual.putAll(file);

                Assert.assertEquals(expected, actual);
            }
        } finally {
            // file.delete();
        }
    }


    @Test
    public void testBondVOInterface() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        ChronicleMapBuilder<CharSequence, BondVOInterface> builder =
                ChronicleMapBuilder.of(CharSequence.class, BondVOInterface.class)
                        .averageKeySize("one".length()).entries(1000);
        try (ChronicleMap<CharSequence, BondVOInterface> expected =
                     builder.create()) {

            final BondVOInterface value = expected.newValueInstance();

            // this will add the entry
            try (WriteContext context = expected.acquireUsingLocked("one", value)) {
                value.setCoupon(8.98);
                BondVOInterface.MarketPx marketPxIntraDayHistoryAt = value.getMarketPxIntraDayHistoryAt(1);

                marketPxIntraDayHistoryAt.setAskPx(12.0);

                assert value == context.value();
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, BondVOInterface> actual = builder.create()) {

                actual.putAll(file);

                Assert.assertEquals(expected.get("one").getCoupon(),
                        actual.get("one").getCoupon(), 0);
            }
        } finally {
            file.delete();
        }
    }
}

