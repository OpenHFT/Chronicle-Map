/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class BasicReplicationTest {
    private static final long TIMEOUT_SECONDS = 10;

    @Test
    void shouldReplicate() throws InterruptedException {
        final ChronicleMapBuilder<String, String> builder = ChronicleMap.of(String.class, String.class)
                .entries(1000).averageKeySize(7).averageValueSize(7);
        try (
                ReplicatedChronicleMap<String, String, Object> mapOne = createReplicatedMap(builder, (byte) 1);
                ReplicatedChronicleMap<String, String, Object> mapTwo = createReplicatedMap(builder, (byte) 2);
                ReplicatedChronicleMap<String, String, Object> mapThree = createReplicatedMap(builder, (byte) 3);
                ReplicationTasks tasks = new ReplicationTasks()
        ) {
            final List<ReplicatedChronicleMap<String, String, Object>> maps = Arrays.asList(mapOne, mapTwo, mapThree);
            for (ReplicatedChronicleMap<String, String, Object> source : maps) {
                // Each source's serialisation buffer stays on its original worker thread.
                ExecutorService scans = tasks.executor(1, "replication-scan-" + source.identifier());
                ReplicationEventProcessor<String, String> processor = new ReplicationEventProcessor<>(tasks, scans, source);
                for (ReplicatedChronicleMap<String, String, Object> destination : maps) {
                    if (source != destination)
                        processor.addDestinationMap(destination);
                }
            }

            final Random random = new Random(0xBAD5EED);
            for (int i = 0; i < 5000; i++) {
                final Map<String, String> map = maps.get(random.nextInt(maps.size()));
                final String key = "key" + random.nextInt(100);
                final String value = "val" + random.nextInt(500);
                map.put(key, value);
            }

            // Deliveries can notify other sources. Wait for the entire causal chain,
            // rather than stopping after one source appears to have an empty queue.
            tasks.awaitQuiescence();
            assertEquals(mapTwo.size(), mapOne.size());
            assertEquals(mapThree.size(), mapOne.size());
            for (String key : mapOne.keySet()) {
                assertEquals(mapTwo.get(key), mapOne.get(key), key);
                assertEquals(mapThree.get(key), mapOne.get(key), key);
            }
        }
    }

    @Test
    void notificationDuringScanIsDrained() throws InterruptedException {
        CountDownLatch scanned = new CountDownLatch(1);
        CountDownLatch finishScan = new CountDownLatch(1);
        AtomicInteger published = new AtomicInteger(1);
        AtomicInteger received = new AtomicInteger();
        try (ReplicationTasks tasks = new ReplicationTasks()) {
            CoalescingDrain drain = new CoalescingDrain(tasks, tasks.executor(1, "notification-control"), () -> {
                int value = published.get();
                received.set(value);
                if (value == 1) {
                    scanned.countDown();
                    await(finishScan);
                }
            });
            try {
                drain.wakeup();
                await(scanned);
                // The destination has been scanned. An end-of-scan clear would lose
                // the notification for this later publication.
                published.set(2);
                drain.wakeup();
            } finally {
                finishScan.countDown();
            }
            tasks.awaitQuiescence();
            assertEquals(2, received.get(), "A notification during a scan must cause another scan");
        }
    }

    @Test
    void deliveryKeepsFurtherScansInFlight() throws InterruptedException {
        try (ReplicationTasks tasks = new ReplicationTasks()) {
            ExecutorService executor = tasks.executor(1, "delivery-control");
            AtomicInteger received = new AtomicInteger();
            CoalescingDrain drain = new CoalescingDrain(tasks, executor, received::incrementAndGet);
            tasks.execute(executor, () -> tasks.execute(executor, drain::wakeup));
            tasks.awaitQuiescence();
            assertEquals(1, received.get(), "Quiescence includes scans scheduled by delayed deliveries");
        }
    }

    @Test
    void reportsDeliveryFailureWithoutHanging() {
        IllegalStateException failure = new IllegalStateException("delivery failed");
        try (ReplicationTasks tasks = new ReplicationTasks()) {
            tasks.execute(tasks.executor(1, "failure-control"), () -> { throw failure; });
            AssertionError reported = assertThrows(AssertionError.class, tasks::awaitQuiescence);
            assertSame(failure, reported.getCause());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timed out waiting for test handoff");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted waiting for test handoff", e);
        }
    }

    private static ReplicatedChronicleMap<String, String, Object> createReplicatedMap(
            ChronicleMapBuilder<String, String> builder, byte replicaId) {
        return (ReplicatedChronicleMap<String, String, Object>) builder.replication(replicaId).create();
    }

    /** Serialises a source's scans without losing notifications received during a scan. */
    private static final class CoalescingDrain {
        private final AtomicInteger notifications = new AtomicInteger();
        private final ReplicationTasks tasks;
        private final ExecutorService executor;
        private final Runnable scan;

        CoalescingDrain(ReplicationTasks tasks, ExecutorService executor, Runnable scan) {
            this.tasks = tasks;
            this.executor = executor;
            this.scan = scan;
        }

        void wakeup() {
            if (notifications.getAndIncrement() == 0)
                tasks.execute(executor, this::drain);
        }

        private void drain() {
            int observed = 1;
            do {
                scan.run();
                // Consume only notifications known before this pass. Later ones survive.
                observed = notifications.addAndGet(-observed);
            } while (observed != 0);
        }
    }

    /** Owns asynchronous work and its storage until every task has terminated. */
    private static final class ReplicationTasks implements AutoCloseable {
        private final List<ExecutorService> executors = new ArrayList<>();
        private final List<Bytes<?>> buffers = new ArrayList<>();
        private int pending;
        private Throwable failure;

        ExecutorService executor(int threads, String name) {
            ExecutorService executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory(name));
            executors.add(executor);
            return executor;
        }

        Bytes<ByteBuffer> buffer() {
            Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer(4096);
            buffers.add(buffer);
            return buffer;
        }

        void execute(ExecutorService executor, Runnable action) {
            synchronized (this) {
                pending++;
            }
            try {
                executor.execute(() -> {
                    Throwable problem = null;
                    try {
                        action.run();
                    } catch (Throwable t) {
                        problem = t;
                    } finally {
                        completed(problem);
                    }
                });
            } catch (RuntimeException | Error e) {
                completed(e);
                throw e;
            }
        }

        private synchronized void completed(Throwable problem) {
            if (problem != null && failure == null)
                failure = problem;
            pending--;
            notifyAll();
        }

        synchronized void awaitQuiescence() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (pending != 0) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Timed out draining replication; pending tasks: " + pending);
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            if (failure != null)
                throw new AssertionError("Asynchronous replication failed", failure);
        }

        @Override
        public void close() {
            executors.forEach(ExecutorService::shutdown);
            boolean interrupted = false;
            boolean terminated = true;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            for (ExecutorService executor : executors) {
                while (!executor.isTerminated()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        terminated = false;
                        executor.shutdownNow();
                        break;
                    }
                    try {
                        executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        interrupted = true;
                        executor.shutdownNow();
                    }
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            // Never release buffers underneath a task which failed to stop.
            assertTrue(terminated, "Replication workers did not terminate");
            buffers.forEach(Bytes::releaseLast);
        }
    }

    private static final class ReplicationEventProcessor<K, V> {
        private final List<IteratorAndDestinationMap<K, V>> destinationMaps = new ArrayList<>();
        private final ReplicatedChronicleMap<K, V, ?> sourceMap;
        private final ReplicationTasks tasks;
        private final CoalescingDrain drain;

        ReplicationEventProcessor(ReplicationTasks tasks, ExecutorService executor, ReplicatedChronicleMap<K, V, ?> sourceMap) {
            this.tasks = tasks;
            this.sourceMap = sourceMap;
            this.drain = new CoalescingDrain(tasks, executor, this::scan);
        }

        void addDestinationMap(ReplicatedChronicleMap<K, V, ?> destinationMap) {
            ReplicatedChronicleMap<K, V, ?>.ModificationIterator iterator =
                    sourceMap.acquireModificationIterator(destinationMap.identifier());
            destinationMaps.add(new IteratorAndDestinationMap<>(tasks, iterator, sourceMap, destinationMap));
            iterator.setModificationNotifier(drain::wakeup);
        }

        private void scan() {
            Collections.shuffle(destinationMaps);
            for (IteratorAndDestinationMap<K, V> destination : destinationMaps) {
                while (destination.modificationIterator.nextEntry(destination, sourceMap.identifier())) {
                    // Drain all currently visible changes for this destination.
                }
            }
        }
    }

    private static final class IteratorAndDestinationMap<K, V> implements Replica.ModificationIterator.Callback {
        private final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator;
        private final ReplicatedChronicleMap<K, V, ?> sourceMap;
        private final ReplicatedChronicleMap<K, V, ?> destinationMap;
        private final ReplicationTasks tasks;
        private final Bytes<ByteBuffer> buffer;
        private final ExecutorService delayedExecutor;
        private final ArrayList<String> keys = new ArrayList<>();

        IteratorAndDestinationMap(ReplicationTasks tasks,
                                  ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator,
                                  ReplicatedChronicleMap<K, V, ?> sourceMap,
                                  ReplicatedChronicleMap<K, V, ?> destinationMap) {
            this.tasks = tasks;
            this.modificationIterator = modificationIterator;
            this.sourceMap = sourceMap;
            this.destinationMap = destinationMap;
            buffer = tasks.buffer();
            delayedExecutor = tasks.executor(1, "replication-delivery");
        }

        @Override
        public void onEntry(ReplicableEntry entry, int chronicleId) {
            buffer.clear();
            keys.clear();
            sourceMap.writeExternalEntry(entry, null, buffer, chronicleId, keys);
            buffer.readPosition(0);
            buffer.readLimit(buffer.writePosition());
            final ByteBuffer message = ByteBuffer.allocate((int) buffer.writePosition());
            buffer.read(message);
            message.position(0);
            tasks.execute(delayedExecutor, () -> {
                Bytes<ByteBuffer> incoming = Bytes.wrapForRead(message);
                try {
                    destinationMap.readExternalEntry(incoming, sourceMap.identifier());
                } finally {
                    incoming.releaseLast();
                }
            });
        }

        @Override
        public void onBootstrapTime(long bootstrapTime, int chronicleId) {
            destinationMap.setRemoteNodeCouldBootstrapFrom((byte) chronicleId, bootstrapTime);
        }
    }
}
