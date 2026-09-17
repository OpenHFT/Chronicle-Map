/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class SharedFileChannelTest {
    private static final int INITIAL_SIZE = 64 << 10;
    private static final int ITERATIONS = 8_192;
    private static final int HEADER_SIZE = 1_612;
    private static final byte[] HEADER = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(0x7007f54e5aa37bf1L).putInt(HEADER_SIZE).array();

    @TempDir
    Path directory;

    @Test
    void canonicalAcquisitionPreservesConcurrentHeaderReads() throws Exception {
        File file = directory.resolve("acquisition.map").toFile().getCanonicalFile();
        RandomAccessFile owner = CanonicalRandomAccessFiles.acquire(file);
        try {
            FileChannel channel = owner.getChannel();
            initialise(owner);
            IOAction acquire = iteration -> {
                RandomAccessFile acquired = CanonicalRandomAccessFiles.acquire(file);
                try {
                    assertSame(owner, acquired, "The regression must exercise a shared handle");
                } finally {
                    CanonicalRandomAccessFiles.release(file);
                }
            };
            runConcurrently(acquire, acquire, readHeader(channel), readHeader(channel),
                    readHeader(channel), readHeader(channel), readHeader(channel), readHeader(channel));
            assertEquals(INITIAL_SIZE, channel.size());
        } finally {
            CanonicalRandomAccessFiles.release(file);
        }
    }

    @Test
    void fileGrowthPreservesConcurrentPositionalReadsAndWrites() throws Exception {
        try (RandomAccessFile owner = new RandomAccessFile(directory.resolve("growth.map").toFile(), "rw")) {
            FileChannel channel = owner.getChannel();
            initialise(owner);
            IOAction grow = iteration -> assertTrue(FileIOUtils.growFile(owner,
                    INITIAL_SIZE + (iteration + 1L) * 4096));
            ByteBuffer data = ByteBuffer.allocate(4).putInt(0x12345678);
            IOAction write = iteration -> {
                data.clear();
                FileIOUtils.writeFully(channel, 128, data);
            };
            runConcurrently(grow, write, readHeader(channel), readHeader(channel),
                    readHeader(channel), readHeader(channel), readHeader(channel), readHeader(channel));
            assertEquals(INITIAL_SIZE + (long) ITERATIONS * 4096, channel.size());
            assertFalse(FileIOUtils.growFile(owner, INITIAL_SIZE), "Growth must not truncate an existing file");
            assertEquals(INITIAL_SIZE + (long) ITERATIONS * 4096, channel.size());
            readHeader(channel).run(0);
            data.clear();
            FileIOUtils.readFully(channel, 128, data);
            assertEquals(4, data.position());
            assertEquals(0x12345678, data.getInt(0));
        }
    }

    private static void initialise(RandomAccessFile file) throws IOException {
        FileIOUtils.growFile(file, INITIAL_SIZE);
        FileIOUtils.writeFully(file.getChannel(), 0, ByteBuffer.wrap(HEADER));
    }

    private static IOAction readHeader(FileChannel channel) {
        ByteBuffer size = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer header = ByteBuffer.allocate(HEADER.length);
        return iteration -> {
            size.clear();
            FileIOUtils.readFully(channel, 8, size);
            assertEquals(4, size.position(), "Short size-word read at iteration " + iteration);
            assertEquals(HEADER_SIZE, size.getInt(0), "Wrong size-word offset at iteration " + iteration);
            header.clear();
            FileIOUtils.readFully(channel, 0, header);
            assertEquals(HEADER.length, header.position(), "Short header read at iteration " + iteration);
            assertArrayEquals(HEADER, header.array(), "Header changed at iteration " + iteration);
        };
    }

    private static void runConcurrently(IOAction... actions) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(actions.length, task -> {
            Thread thread = new Thread(task, "shared-file-io-regression");
            thread.setDaemon(true);
            return thread;
        });
        CountDownLatch ready = new CountDownLatch(actions.length);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> workers = new ArrayList<>();
        Throwable failure = null;
        try {
            for (IOAction action : actions) {
                workers.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < ITERATIONS && !stop.get(); i++)
                        action.run(i);
                    return null;
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers did not start");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            start.countDown();
            for (Future<?> worker : workers)
                worker.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            failure = e.getCause();
            if (failure instanceof Error)
                throw (Error) failure;
            throw (Exception) failure;
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            stop.set(true);
            start.countDown();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Workers did not terminate");
                }
            } catch (Exception | Error cleanupFailure) {
                executor.shutdownNow();
                if (failure == null)
                    throw cleanupFailure;
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    @FunctionalInterface
    private interface IOAction {
        void run(int iteration) throws IOException;
    }
}
