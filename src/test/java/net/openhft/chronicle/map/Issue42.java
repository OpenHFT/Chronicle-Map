package net.openhft.chronicle.map;

import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;

import static net.openhft.chronicle.map.Alignment.NO_ALIGNMENT;
import static net.openhft.chronicle.map.Builder.getPersistenceFile;

public class Issue42 {
    @Test
    public void crashJVMWindowsTest() throws IOException {
        final ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entryAndValueAlignment(NO_ALIGNMENT)
                .entrySize(18)
                .entries(15000000)
                .minSegments(128)
                .create(getPersistenceFile());

        for (int i = 0; i < 10000000; ++i) {
            String s = String.valueOf(i);
            map.put(s, s);
        }

        for (int i = 0; i < 10000000; ++i) {
            String s = String.valueOf(i);
            Assert.assertEquals(s, map.get(s));
        }

        map.close();
    }
}
