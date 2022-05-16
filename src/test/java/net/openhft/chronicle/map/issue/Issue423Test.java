package net.openhft.chronicle.map.issue;

import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static org.junit.Assert.assertNotNull;

public class Issue423Test {

    @Test
    public void reproduce() throws IOException {
        final File file = new File("issue423");

        try {
            ChronicleMap<Integer, Integer> map = ChronicleMapBuilder.of(Integer.class, Integer.class)
                    // This will throw an IllegalArgumentException as .entries() are not called
                    .createPersistedTo(file);

        } catch (IllegalStateException ignored) {
            // expected path
        }
        final RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(file.getCanonicalFile());
        final FileChannel fileChannel = raf.getChannel();

        try (FileLock lock = fileChannel.tryLock()) {
            // Make sure we can lock (hence the file was not previously locked)
            assertNotNull(lock);
        }

    }
}