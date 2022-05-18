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
import static org.junit.Assert.assertTrue;

public class Issue423LockFileRemainsLockedOnWindowsIfBuilderThrowsTest {

    @Test
    public void reproduce() throws IOException {
        final File file = new File("issue423");

        try {
            ChronicleMap<Integer, CharSequence> map = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    // This will throw an IllegalArgumentException as the value seize is not known
                    .entries(100)
                    .createPersistedTo(file);

        } catch (IllegalStateException ignored) {
            // expected path
        }

        try (RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(file.getCanonicalFile());
             FileChannel fileChannel = raf.getChannel();) {

            try (FileLock lock = fileChannel.tryLock()) {
                // Make sure we can lock (hence the file was not previously locked)
                assertNotNull(lock);
            }
        }

        // Make sure the file can be deleted despite an Exception was thrown by the builder
        assertTrue(file.delete());

    }
}