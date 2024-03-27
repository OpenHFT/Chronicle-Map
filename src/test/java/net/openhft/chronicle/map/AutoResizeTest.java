package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AutoResizeTest {

    @BeforeClass
    public static void setup() {
        Jvm.setExceptionHandlers(null, null, null);
    }

    @AfterClass
    public static void reset() {
        Jvm.resetExceptionHandlers();
    }

    /**
     * test that the auto resizes are not zero upon a restart
     *
     * @throws IOException IOException
     */
    @Test
    public void testAutoResizeNotZeroUponRestart() throws IOException {

        File cmap = File.createTempFile("chron", "cmap");

        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .entries(100).actualSegments(10).maxBloatFactor(10).replication((byte) 1)
                .createPersistedTo(cmap)) {

            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0, actual);
        }

        // if the file already exists  it will reuse the existing settings, set above

        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .createPersistedTo(cmap)) {
            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0, actual);
        }
    }

    @Test
    public void testAutoResizeNotZeroUponRestart2() {

        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .entries(100).actualSegments(10).maxBloatFactor(10).replication((byte) 1)
                .create()) {

            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0, actual);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeReplication() {
	ChronicleMapBuilder.of(String.class, String.class).replication((byte) -1);
    }
}
