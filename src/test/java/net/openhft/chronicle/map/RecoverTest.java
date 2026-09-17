/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.hash.ChecksumEntry;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.map.ChronicleMapTest.getPersistenceFile;
import static org.junit.jupiter.api.Assertions.*;

class RecoverTest {

    private static final Logger LOG = LoggerFactory.getLogger(RecoverTest.class);

    ReplicatedChronicleMap<Integer, Integer, ?> map;

    @Test
    @Disabled("HCOLL-422")
    void recoverTest() throws IOException, ExecutionException, InterruptedException {
        File mapFile = File.createTempFile("recoverTestFile", ".map");
        mapFile.deleteOnExit();

        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(2)
                .actualSegments(1)
                .checksumEntries(true);
        final ChronicleHashBuilderPrivateAPI<?, ?> privateAPI = Jvm.getValue(builder, "privateAPI");
        privateAPI.replication((byte) 1);
        privateAPI.cleanupRemovedEntries(false);

        map = (ReplicatedChronicleMap<Integer, Integer, ?>) builder.createPersistedTo(mapFile);

        map.acquireModificationIterator((byte) 2);

        // acquires read lock successfully
        assertNull(map.get(0));

        ExecutorService executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("recoverTest"));

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

        map = (ReplicatedChronicleMap<Integer, Integer, ?>)
                builder.recoverPersistedTo(mapFile, true);

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
                    ChronicleMapBuilder<Integer, Integer> recoverBuilder = ChronicleMap
                            .of(Integer.class, Integer.class);
                    final ChronicleHashBuilderPrivateAPI<?, ?> recoverPrivateAPI = Jvm.getValue(recoverBuilder, "privateAPI");

                    recoverPrivateAPI.replication((byte) 1);
                    recoverPrivateAPI.cleanupRemovedEntries(false);
                    try (ChronicleMap<Integer, Integer> recovered =
                                 recoverBuilder.recoverPersistedTo(mapFile, false)) {
                        recovered.put(1, 1);
                        recovered.put(2, 2);
                        recovered.remove(1);
                    }
                }
            }
        }
    }

    @Test
    void testCorruptedEntryRecovery() throws IOException {
        File file = getPersistenceFile();
        try (ChronicleMap<Integer, LongValue> map = ChronicleMap
                .of(Integer.class, LongValue.class)
                .entries(1)
                .createPersistedTo(file)) {

            LongValue value = Values.newHeapInstance(LongValue.class);
            value.setValue(42);
            map.put(1, value);

            try (ExternalMapQueryContext<Integer, LongValue, ?> c = map.queryContext(1)) {
                // Update lock required for calling ChecksumEntry.checkSum()
                c.updateLock().lock();
                MapEntry<Integer, LongValue> entry = c.entry();
                assertNotNull(entry);
                ChecksumEntry checksumEntry = (ChecksumEntry) entry;
                assertTrue(checksumEntry.checkSum());

                // to access off-heap bytes, should call value().getUsing() with Native value
                // provided. Simple get() return Heap value by default
                LongValue nativeValue =
                        entry.value().getUsing(Values.newNativeReference(LongValue.class));
                // This value bytes update bypass Chronicle Map internals, so checksum is not
                // updated automatically
                nativeValue.setValue(43);
                assertFalse(checksumEntry.checkSum());
            }
        }

        AtomicInteger corruptionCounter = new AtomicInteger(0);
        ChronicleHashCorruption.Listener corruptionListener =
                corruption -> corruptionCounter.incrementAndGet();
        //noinspection EmptyTryBlock
        try (ChronicleMap<Integer, LongValue> ignore = ChronicleMap
                .of(Integer.class, LongValue.class)
                .entries(1)
                .recoverPersistedTo(file, true, corruptionListener)) {
            assertNotNull(ignore);
        }
        assertTrue(corruptionCounter.get() > 0);
    }
}
