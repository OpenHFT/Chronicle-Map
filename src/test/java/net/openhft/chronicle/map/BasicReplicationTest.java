/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BasicReplicationTest {

    private static byte asByte(final int i) {
        return (byte) i;
    }

    @Test
    public void shouldReplicate() {
        final ChronicleMapBuilder<String, String> builder = ChronicleMap.of(String.class, String.class)
                .entries(1000).averageKeySize(7).averageValueSize(7);
        try (
                ReplicatedChronicleMap<String, String, Object> mapOne = createReplicatedMap(builder, asByte(1));
                ReplicatedChronicleMap<String, String, Object> mapTwo = createReplicatedMap(builder, asByte(2));
                ReplicatedChronicleMap<String, String, Object> mapThree = createReplicatedMap(builder, asByte(3))
        ) {

            final ReplicationEventProcessor<String, String> processorOne =
                    new ReplicationEventProcessor<>();
            processorOne.addDestinationMap(mapOne.acquireModificationIterator(asByte(2)), mapOne, mapTwo);
            processorOne.addDestinationMap(mapOne.acquireModificationIterator(asByte(3)), mapOne, mapThree);

            final ReplicationEventProcessor<String, String> processorTwo =
                    new ReplicationEventProcessor<>();
            processorTwo.addDestinationMap(mapTwo.acquireModificationIterator(asByte(1)), mapTwo, mapOne);
            processorTwo.addDestinationMap(mapTwo.acquireModificationIterator(asByte(3)), mapTwo, mapThree);

            final ReplicationEventProcessor<String, String> processorThree =
                    new ReplicationEventProcessor<>();
            processorThree.addDestinationMap(mapThree.acquireModificationIterator(asByte(1)), mapThree, mapOne);
            processorThree.addDestinationMap(mapThree.acquireModificationIterator(asByte(2)), mapThree, mapTwo);

            final ExecutorService executorService = Executors.newFixedThreadPool(3);
            executorService.submit(processorOne::processPendingChangesLoop);
            executorService.submit(processorTwo::processPendingChangesLoop);
            executorService.submit(processorThree::processPendingChangesLoop);

            final Map[] maps = new Map[]{mapOne, mapTwo, mapThree};
            final Random random = new Random(0xBAD5EED);
            for (int i = 0; i < 5000; i++) {
                final int mapIndex = random.nextInt(maps.length);
                final Map<String, String> map = maps[mapIndex];
                final String key = "key" + random.nextInt(100);
                final String value = "val" + random.nextInt(500);
                map.put(key, value);
            }

            waitForBacklog(processorOne);
            waitForBacklog(processorTwo);
            waitForBacklog(processorThree);

            executorService.shutdownNow();

            waitForFinish(processorOne);
            waitForFinish(processorTwo);
            waitForFinish(processorThree);

            assertThat(mapOne.size(), is(equalTo(mapTwo.size())));
            assertThat(mapOne.size(), is(equalTo(mapThree.size())));

            for (String key : mapOne.keySet()) {
                final String mapOneValue = mapOne.get(key);
                final String mapTwoValue = mapTwo.get(key);
                final String mapThreeValue = mapThree.get(key);

                assertThat(mapOneValue, CoreMatchers.equalTo(mapTwoValue));
                assertThat(mapOneValue, CoreMatchers.equalTo(mapThreeValue));
            }
        }
    }

    private void waitForBacklog(final ReplicationEventProcessor<String, String> p) {
        while (!p.queueEmpty.get()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
        }
    }

    private void waitForFinish(final ReplicationEventProcessor<String, String> processor) {
        for (IteratorAndDestinationMap<String, String> destinationMap : processor.destinationMaps) {
            while (destinationMap.messagesInflight.get() != 0 || !processor.stopped.get()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        }
    }

    private ReplicatedChronicleMap<String, String, Object>
    createReplicatedMap(final ChronicleMapBuilder<String, String> builder, final byte replicaId) {
        final ChronicleMap<String, String> map = builder.replication(replicaId).create();
        return (ReplicatedChronicleMap<String, String, Object>) map;
    }

    private static final class ReplicationEventProcessor<K, V> {
        private final List<IteratorAndDestinationMap<K, V>> destinationMaps = new ArrayList<>();
        private final AtomicBoolean queueEmpty = new AtomicBoolean(true);
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private ReplicatedChronicleMap<K, V, ?> sourceMap;

        void addDestinationMap(final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator,
                               final ReplicatedChronicleMap<K, V, ?> sourceMap,
                               final ReplicatedChronicleMap<K, V, ?> destinationMap) {
            destinationMaps.add(new IteratorAndDestinationMap<K, V>(modificationIterator, sourceMap, destinationMap));
            if (this.sourceMap != null && this.sourceMap != sourceMap) {
                throw new IllegalArgumentException("All iterators must belong to the same source map");
            }
            this.sourceMap = sourceMap;
            modificationIterator.setModificationNotifier(this::wakeup);
        }

        void wakeup() {
            queueEmpty.set(false);
        }

        void processPendingChangesLoop() {

            try {

                while (!Thread.currentThread().isInterrupted()) {
                    while (queueEmpty.get() && !Thread.currentThread().isInterrupted()) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(7));
                    }

                    Collections.shuffle(destinationMaps);

                    for (IteratorAndDestinationMap<K, V> iteratorAndDestinationMap : destinationMaps) {
                        while (iteratorAndDestinationMap.modificationIterator.nextEntry(
                                iteratorAndDestinationMap, sourceMap.identifier())) {
                        }
                    }

                    queueEmpty.compareAndSet(false, true);
                }
            } finally {
                stopped.set(true);
            }
        }
    }

    private static final class IteratorAndDestinationMap<K, V> implements Replica.ModificationIterator.Callback {
        private final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator;
        private final ReplicatedChronicleMap<K, V, ?> sourceMap;
        private final ReplicatedChronicleMap<K, V, ?> destinationMap;
        private final Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer(4096);
        private final ExecutorService delayedExecutor = Executors.newSingleThreadExecutor();
        private final AtomicInteger messagesInflight = new AtomicInteger(0);

        IteratorAndDestinationMap(final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator,
                                  final ReplicatedChronicleMap<K, V, ?> sourceMap,
                                  final ReplicatedChronicleMap<K, V, ?> destinationMap) {
            this.modificationIterator = modificationIterator;
            this.sourceMap = sourceMap;
            this.destinationMap = destinationMap;
        }

        @Override
        public void onEntry(final ReplicableEntry entry, final int chronicleId) {
            try {
                buffer.clear();
                sourceMap.writeExternalEntry(entry, null, buffer, chronicleId);

                buffer.readPosition(0);
                buffer.readLimit(buffer.writePosition());
                final ByteBuffer message = ByteBuffer.allocate((int) buffer.writePosition());
                buffer.read(message);
                message.position(0);
                messagesInflight.incrementAndGet();
                delayedExecutor.submit(() -> {
                    try {
                        final Bytes<ByteBuffer> tmp = Bytes.elasticByteBuffer(128);
                        while (message.remaining() != 0) {
                            tmp.writeByte(message.get());
                        }

                        destinationMap.readExternalEntry(tmp, sourceMap.identifier());
                        messagesInflight.decrementAndGet();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                });
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onBootstrapTime(final long bootstrapTime, final int chronicleId) {
            destinationMap.setRemoteNodeCouldBootstrapFrom((byte) chronicleId, bootstrapTime);
        }
    }
}