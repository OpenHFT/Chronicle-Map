package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AutoResizeTest {

    static {
        Jvm.setExceptionHandlers(null, null, null);
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
                .createOrRecoverPersistedTo(cmap)) {

            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0.0, actual);
            System.out.println(actual);
        }

        // if the file already exists  it will reuse the existing settings, set above

        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .createOrRecoverPersistedTo(cmap)) {
            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0.0, map.remainingAutoResizes());
            System.out.println(actual);
        }

    }

    @Test
    public void testAutoResizeNotZeroUponRestart2() throws IOException {

        try (ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .entries(100).actualSegments(10).maxBloatFactor(10).replication((byte) 1)
                .create()) {

            int actual = map.remainingAutoResizes();
            Assert.assertNotEquals(0.0, actual);
            System.out.println(actual);
        }

    }

}
