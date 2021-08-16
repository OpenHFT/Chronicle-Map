package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@Ignore("https://github.com/OpenHFT/Chronicle-Map/issues/317")
public class HugeSparseMapTest {

    private static ChronicleMap<CharSequence, CharSequence> createMap(boolean sparseFile) throws IOException {
        File file = IOTools.createTempFile("huge-map");
        try {
            file.deleteOnExit();
            ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                    .of(CharSequence.class, CharSequence.class)
                    .averageKeySize(8);
            if (sparseFile)
                builder.entries(3_000_000_000L).averageValueSize(16); // 216 GB
            else
                builder.entries(10_000_000_000L).averageValueSize(640); // 16 TB
//                    .entries(3_000_000_000L).averageValueSize(200) // 2 TB
            builder.sparseFile(sparseFile); // ~16 TB.
            ChronicleMap<CharSequence, CharSequence> map = builder.createPersistedTo(file);
            return map;
        } catch (Throwable t) {
            file.delete();
            throw t;
        }
    }

    @Test
    public void hugeSparseMap() throws IOException {
        assumeTrue(OS.isLinux());
        try (ChronicleMap<CharSequence, CharSequence> map = createMap(true)) {
            map.put("hi", "there");
            assertEquals("there", map.get("hi").toString());
        }
    }

    @Test(expected = IOException.class)
    public void hugeAllocatedMap() throws IOException {
        assumeTrue(OS.isLinux());
        try (ChronicleMap<CharSequence, CharSequence> map = createMap(false)) {
            map.put("hi", "there");
            assertEquals("there", map.get("hi").toString());
        }
    }
}
