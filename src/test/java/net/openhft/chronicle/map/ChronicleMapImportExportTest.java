package net.openhft.chronicle.map;

import junit.framework.Assert;
import net.openhft.lang.values.LongValue;
import net.openhft.lang.values.LongValue$$Native;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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

    @Ignore("this type of off heap reference is not currently supported")
    @Test
    public void testWithLongValue() throws IOException, InterruptedException {

        File file = new File(TMP + "/chronicle-map-" + System.nanoTime() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        try (ChronicleMap<CharSequence, LongValue> expected = ChronicleMapBuilder.of(CharSequence.class, LongValue
                .class)
                .create()) {

            LongValue value = new LongValue$$Native();

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

}
