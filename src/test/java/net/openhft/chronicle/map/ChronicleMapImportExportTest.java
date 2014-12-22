package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
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


        try (ChronicleMap<Integer, String> expected = ChronicleMapBuilder.of(Integer.class,
                String.class).create()) {

            expected.put(1, "one");
            expected.put(2, "two");

            expected.getAll(file2);
            expected.putAll(file2);


            Assert.assertEquals(2, expected.size());
            Assert.assertEquals("one", expected.get(1));
            Assert.assertEquals("two", expected.get(2));
        }


        //file.deleteOnExit();
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
        //file.deleteOnExit();

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
            // file.delete();
        }
    }


    @Test
    public void testBondVOInterface() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
      //  file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        try (ChronicleMap<CharSequence, BondVOInterface> expected =
                     ChronicleMapBuilder.of(CharSequence.class, BondVOInterface.class).create()) {

            final BondVOInterface value = expected.newValueInstance();

            // this will add the entry
            try (WriteContext context = expected.acquireUsingLocked("one", value)) {
                value.setCoupon(8.98);
                assert value == context.value();
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, BondVOInterface> actual =
                         ChronicleMapBuilder.of(CharSequence.class,
                                 DataValueClasses.directClassFor(BondVOInterface.class)).create()) {

                actual.putAll(file);

                Assert.assertEquals(expected.get("one").getCoupon(), actual.get("one").getCoupon(), 0);
            }
        } finally {
     //       file.delete();
        }
    }
}

