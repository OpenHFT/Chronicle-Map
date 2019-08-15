package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by catst01 on 24/10/2018.
 */
public class MissSizedMapsTest {

    @Test(timeout = 60000)
    public void testSmallEntries() throws IOException, URISyntaxException {
        ChronicleMap<String, String> actual = ChronicleMapBuilder.of(String.class, String.class)
                .averageKey("D-6.0149935894066442E18").averageValue("226|16533|4|1|1|testHarness").entries(150 << 10)
                .createPersistedTo(File.createTempFile("chronicle", "cmap"));
        check(actual);
    }

    private void check(final ChronicleMap<String, String> actual) throws
            IOException, URISyntaxException {
        URI uri = MissSizedMapsTest.class.getResource("/input.txt").toURI();

        List<String> strings = Files.readAllLines(Paths.get(uri));
        Map<String, String> expected = new HashMap<>();

        int maxKey = 0;
        int maxValue = 0;
        for (String s : strings) {
            String[] split = s.split("&");
            expected.put(split[0], split[1]);
            actual.put(split[0], split[1]);
            maxKey = Integer.max(maxKey, split[0].length());
            maxValue = Integer.max(maxValue, split[1].length());

        }

        Assert.assertEquals(actual.size(), expected.size());
        Assert.assertEquals(actual.keySet().size(), expected.keySet().size());

        for (String key : actual.keySet()) {
            if (!expected.containsKey(key)) {
                Assert.fail(key + " not in key set but in map expected");
            }
        }

        for (String key : expected.keySet()) {
            if (!actual.containsKey(key)) {
                Assert.fail(key + " not in key set but in map actual");
            }
        }
    }

}
