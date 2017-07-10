package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertThat;

public class BasicReplicationTest {

    @Ignore
    @Test
    public void shouldReplicate() throws Exception {
        final ChronicleMapBuilder<String, String> builder = ChronicleMap.of(String.class, String.class)
                .entries(1000).averageKeySize(7).averageValueSize(7);


        try (
                ReplicatedChronicleMap<String, String, Object> mapOne = createReplicatedMap(builder, asByte(1));
                ReplicatedChronicleMap<String, String, Object> mapTwo = createReplicatedMap(builder, asByte(2));
                ReplicatedChronicleMap<String, String, Object> mapThree = createReplicatedMap(builder, asByte(3));
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
            executorService.submit(processorTwo::processPendingChangesLoop);

            final Map[] maps = new Map[] {mapOne, mapTwo, mapThree};

            final Random random = new Random(0xBAD5EED);
            for (int i = 0; i < 50; i++) {
                final int mapIndex = random.nextInt(maps.length);
                final Map map = maps[mapIndex];
                final String key = "key" + random.nextInt(100);
                final String value = "val" + random.nextInt(500);
                map.put(key, value);
                System.out.printf("map %d, put(%s, %s)%n", mapIndex, key, value);
            }

            LockSupport.parkNanos(TimeUnit.DAYS.toNanos(5L));
            executorService.shutdownNow();

            for(String key : mapOne.keySet()) {
                final String mapOneValue = mapOne.get(key);
                final String mapTwoValue = mapTwo.get(key);
                final String mapThreeValue = mapThree.get(key);

                assertThat(mapOneValue, CoreMatchers.equalTo(mapTwoValue));
                assertThat(mapOneValue, CoreMatchers.equalTo(mapThreeValue));
            }
        }
    }

    private static byte asByte(final int i) {
        return (byte) i;
    }

    private static final class ReplicationEventProcessor<K, V> {
        private final List<IteratorAndDestinationMap<K, V>> destinationMaps = new ArrayList<>();
        private final AtomicBoolean queueEmpty = new AtomicBoolean(true);
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

            while(!Thread.currentThread().isInterrupted()) {
                while (queueEmpty.get()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(7));
                }

                Collections.shuffle(destinationMaps);

                for (IteratorAndDestinationMap<K, V> iteratorAndDestinationMap : destinationMaps) {
                    while (iteratorAndDestinationMap.modificationIterator.hasNext()) {
                        iteratorAndDestinationMap.modificationIterator.nextEntry(iteratorAndDestinationMap, 17);
                    }
                }

                queueEmpty.compareAndSet(false, true);
            }
        }
    }

    private static final class IteratorAndDestinationMap<K, V> implements Replica.ModificationIterator.Callback {
        private final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator;
        private final ReplicatedChronicleMap<K, V, ?> sourceMap;
        private final ReplicatedChronicleMap<K, V, ?> destinationMap;
        private final Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();

        IteratorAndDestinationMap(final ReplicatedChronicleMap<K, V, ?>.ModificationIterator modificationIterator,
                                  final ReplicatedChronicleMap<K, V, ?> sourceMap,
                                  final ReplicatedChronicleMap<K, V, ?> destinationMap) {
            this.modificationIterator = modificationIterator;
            this.sourceMap = sourceMap;
            this.destinationMap = destinationMap;
        }

        @Override
        public void onEntry(final ReplicableEntry entry, final int chronicleId) {

            try (CompiledReplicatedMapQueryContext<K, V, ?> iCtx = sourceMap.mapContext()) {
                iCtx.updateLock().lock();
                try {
                    sourceMap.writeExternalEntry(entry, null, buffer, chronicleId);
                } finally {
                    iCtx.updateLock().unlock();
                }
            } catch(Throwable e) {
                e.printStackTrace();
            }
            final long writeLimit = buffer.writeLimit();

            buffer.readPosition(0);
            buffer.readLimit(writeLimit);

            destinationMap.readExternalEntry(buffer, sourceMap.identifier());
        }

        @Override
        public void onBootstrapTime(final long bootstrapTime, final int chronicleId) {

        }
    }


    private ReplicatedChronicleMap<String, String, Object>
        createReplicatedMap(final ChronicleMapBuilder<String, String> builder, final byte replicaId) {
        final ChronicleMapBuilderPrivateAPI<String, String> privateBuilder =
                new ChronicleMapBuilderPrivateAPI<>(builder);
        privateBuilder.replication(replicaId);
        final ChronicleMap<String, String> map = builder.create();
        return (ReplicatedChronicleMap<String, String, Object>) map;
    }

    private static final class ReplicationCallback implements Replica.ModificationIterator.Callback {

        private final ReplicatedChronicleMap<String, String, Object> sourceMap;
        private final ReplicatedChronicleMap<String, String, Object> destinationMap;

        ReplicationCallback(final ReplicatedChronicleMap<String, String, Object> sourceMap,
                            final ReplicatedChronicleMap<String, String, Object> destinationMap) {
            this.sourceMap = sourceMap;
            this.destinationMap = destinationMap;
        }

        @Override
        public void onEntry(final ReplicableEntry entry, final int chronicleId) {
            final boolean changed = entry.isChanged();
            final byte originIdentifier = entry.originIdentifier();
            final long originTimestamp = entry.originTimestamp();

            System.out.printf("onEntry(%s, %d, %d)%n", changed, originIdentifier, originTimestamp);

            final boolean identifierCheck = sourceMap.identifierCheck(entry, chronicleId);
            final Bytes<ByteBuffer> destination = Bytes.elasticByteBuffer();
            sourceMap.writeExternalEntry(entry, null, destination, chronicleId);
            System.out.println(identifierCheck);
            System.out.println("destination: " + destination);
            final long writeLimit = destination.writeLimit();
            destination.readPosition(0);
            destination.readLimit(writeLimit);

            destinationMap.readExternalEntry(destination, sourceMap.identifier());
        }

        @Override
        public void onBootstrapTime(final long bootstrapTime, final int chronicleId) {
            System.out.printf("onBootstrapTime(%d, %d)%n", bootstrapTime, chronicleId);
        }
    }
}
