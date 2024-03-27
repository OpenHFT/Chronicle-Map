package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class Issue354bTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void build_toFile() throws IOException {
        String baseDirectory = testFolder.getRoot().toString();
        File file = new File(baseDirectory, "chronicle.dat");
        ChronicleMap<LongValue, LongValue> map = ChronicleMapBuilder.of(LongValue.class, LongValue.class)
                .name("test")
                .entries(5)
                .createPersistedTo(file);

        assertTrue(file.isFile());
    }

}
