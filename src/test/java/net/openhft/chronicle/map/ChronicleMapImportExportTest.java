package net.openhft.chronicle.map;

import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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

            System.out.println(expected);
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



}
