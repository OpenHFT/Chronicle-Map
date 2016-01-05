/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNull;

public class RecoverTest {

    Logger LOG = LoggerFactory.getLogger(RecoverTest.class);

    ReplicatedChronicleMap<Integer, Integer, ?> map;

    @Test
    public void recoverTest() throws IOException, ExecutionException, InterruptedException {
        File mapFile = File.createTempFile("recoverTestFile", ".map");
        mapFile.deleteOnExit();

        map = (ReplicatedChronicleMap<Integer, Integer, ?>) ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(2)
                .actualSegments(1)
                .checksumEntries(true)
                .replication((byte) 1)
                .cleanupRemovedEntries(false)
                .createPersistedTo(mapFile);

        map.acquireModificationIterator((byte) 2);

        // acquires read lock successfully
        assertNull(map.get(0));

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(() -> {
            ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(0);
            c.writeLock().lock();
        }).get();

        try {
            map.get(0);
            throw new AssertionError("Expected dead lock exception");
        } catch (Exception expected) {
            // do nothing
        }

        map.close();

        map = (ReplicatedChronicleMap<Integer, Integer, ?>) ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(2)
                .actualSegments(1)
                .checksumEntries(true)
                .replication((byte) 1)
                .cleanupRemovedEntries(false)
                .recoverPersistedTo(mapFile, true);

        // acquires read lock successfully
        assertNull(map.get(0));

        map.put(1, 1);
        map.put(2, 2);
        map.remove(1);

        long segmentHeadersOffset = this.map.segmentHeadersOffset;
        map.close();

        try (RandomAccessFile raf = new RandomAccessFile(mapFile, "rw")) {
            FileChannel ch = raf.getChannel();
            MappedByteBuffer mapBB = ch.map(FileChannel.MapMode.READ_WRITE, 0, mapFile.length());
            for (long offset = segmentHeadersOffset; offset < mapFile.length();
                 offset += 8) {
                for (int bit = 0; bit < 64; bit++) {
                    LOG.error("flip bit {} of word at {}", bit, offset);
                    mapBB.putLong((int) offset, mapBB.getLong((int) offset) ^ (1L << bit));
                    try (ChronicleMap<Integer, Integer> recovered = ChronicleMap
                            .of(Integer.class, Integer.class)
                            .replication((byte) 1)
                            .cleanupRemovedEntries(false)
                            .recoverPersistedTo(mapFile, false)) {
                        recovered.put(1, 1);
                        recovered.put(2, 2);
                        recovered.remove(1);
                    }
                }
            }
        }
    }
}
