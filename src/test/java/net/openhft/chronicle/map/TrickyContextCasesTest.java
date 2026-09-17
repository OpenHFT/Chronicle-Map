/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.hash.impl.BigSegmentHeader;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static net.openhft.chronicle.algo.hashing.LongHashFunction.xx_r39;
import static org.junit.jupiter.api.Assertions.*;

class TrickyContextCasesTest {

    @Test
    void nestedContextsSameKeyTest() {
        try (ChronicleMap<Integer, IntValue> map = ChronicleMapBuilder
                .of(Integer.class, IntValue.class).entries(1).create()) {
            IntValue v = Values.newHeapInstance(IntValue.class);
            v.setValue(2);
            map.put(1, v);
            try (ExternalMapQueryContext<Integer, IntValue, ?> q = map.queryContext(1)) {
                q.writeLock().lock();
                IntValue v2 = q.entry().value().get();
                assertEquals(2, v2.getValue());
                IllegalStateException failure = assertThrows(IllegalStateException.class, () -> map.remove(1));
                assertTrue(failure.getMessage().contains("Nested same-thread contexts cannot access the same key"));
                assertEquals(2, v2.getValue());
            }
            assertEquals(1, map.size());
            assertEquals(2, map.get(1).getValue());
        }
    }

    @Test
    @SuppressWarnings("try") // The resource exists only to order worker termination and map close.
    void testPutShouldBeWriteLocked() throws Exception {
        ChronicleMap<Integer, byte[]> map = ChronicleMapBuilder
                .of(Integer.class, byte[].class)
                .averageValue(new byte[1])
                .entries(100).actualSegments(1).create();
        ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("write-lock-test"));
        // Close contexts first, then join the worker before releasing its mapped memory.
        // try-with-resources retains any assertion failure if worker cleanup also fails.
        try (AutoCloseable cleanup = () -> {
            stopWorker(executor);
            map.close();
        }) {
            map.put(1, new byte[]{1});
            map.put(2, new byte[]{2});
            try (ExternalMapQueryContext<Integer, byte[], ?> q = map.queryContext(1)) {
                MapEntry<Integer, byte[]> entry = q.entry(); // acquires read lock implicitly
                assertNotNull(entry);
                Future<?> write = executor.submit(() -> map.put(1, new byte[]{1, 2, 3, 4, 5}));
                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> write.get(BigSegmentHeader.LOCK_TIMEOUT_SECONDS + 10L, TimeUnit.SECONDS));
                assertEquals(InterProcessDeadLockException.class, failure.getCause().getClass());
                assertTrue(q.readLock().isHeldByCurrentThread());
            }
            // Relocation publishes its new entry before acquiring the write lock.
            // Lock rejection does not establish rollback of the attempted value change.
            assertArrayEquals(new byte[]{2}, map.get(2));
            assertEquals(2, map.size());
        }
    }

    private static void stopWorker(ExecutorService executor) {
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            executor.shutdownNow();
            while (!executor.isTerminated()) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Write-lock worker did not terminate; its map has not been closed");
                try {
                    executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Test
    void testHashCollision() {
        try (ChronicleMap<ByteBuffer, Integer> map = ChronicleMap
                .of(ByteBuffer.class, Integer.class)
                .constantKeySizeBySample(ByteBuffer.allocate(128))
                .entries(2)
                .create()) {
            ByteBuffer key1 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key1.putLong(0, 1);
            key1.putLong(32, 2);

            ByteBuffer key2 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key2.putLong(0, 1 + 0xBA79078168D4BAFL);
            key2.putLong(32, 2 + 0x9C90005B80000000L);

            ByteBuffer key3 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key3.putLong(0, 1 + 0xBA79078168D4BAFL * 2);
            key3.putLong(32, 2 + 0x9C90005B80000000L * 2);

            assertEquals(xx_r39().hashBytes(key1), xx_r39().hashBytes(key2));
            assertEquals(xx_r39().hashBytes(key1), xx_r39().hashBytes(key3));

            try (ExternalMapQueryContext<ByteBuffer, Integer, ?> c1 = map.queryContext(key1)) {
                c1.writeLock().lock();
                c1.insert(c1.absentEntry(), c1.wrapValueAsData(1));

                try (ExternalMapQueryContext<ByteBuffer, Integer, ?> c2 = map.queryContext(key2)) {
                    c2.writeLock().lock();
                    c2.insert(c2.absentEntry(), c2.wrapValueAsData(2));

                    c1.remove(c1.entry());

                    map.put(key3, 3);

                    c2.replaceValue(c2.entry(), c2.wrapValueAsData(22));
                }
            }

            assertEquals(2, map.size());
            assertEquals((Integer) 22, map.get(key2));
            assertEquals((Integer) 3, map.get(key3));
        }
    }
}
