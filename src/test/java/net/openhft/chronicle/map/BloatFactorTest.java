package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.map.example.StringArrayExampleTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BloatFactorTest {

    private static final int EXPECTED_MAX_BLOAT_FACTOR = 100;

    /**
     * test that the bloat factor set, remains after a restart
     *
     * @throws IOException IOException
     */
    @Test
    public void bloatFactorTest() throws IOException {

        File cmap = File.createTempFile("chron", "cmap");

        try (ChronicleMap<Integer, StringArrayExampleTest.CharSequenceArray> map = ChronicleMapBuilder
                .of(Integer.class, StringArrayExampleTest.CharSequenceArray.class)
                .entries(100).maxBloatFactor(EXPECTED_MAX_BLOAT_FACTOR)
                .createPersistedTo(cmap)) {

            double maxBloatFactor = Jvm.getValue(map, "maxBloatFactor");
            Assert.assertEquals(EXPECTED_MAX_BLOAT_FACTOR, maxBloatFactor, 0.0);

        }

        // if the file already exists  it will reuse the existing settings, set above

        try (ChronicleMap<Integer, StringArrayExampleTest.CharSequenceArray> map = ChronicleMapBuilder
                .of(Integer.class, StringArrayExampleTest.CharSequenceArray.class)
                .createPersistedTo(cmap)) {

            double maxBloatFactor = Jvm.getValue(map, "maxBloatFactor");
            Assert.assertEquals(EXPECTED_MAX_BLOAT_FACTOR, maxBloatFactor, 0.0);

        }
    }
}
