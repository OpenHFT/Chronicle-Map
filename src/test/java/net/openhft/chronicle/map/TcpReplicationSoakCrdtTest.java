package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.TcpReplicationSoakCrdtTest.GrowOnlySetValuedMapEntryOperations.growOnlySetValuedMapEntryOperations;
import static net.openhft.chronicle.map.TcpReplicationSoakCrdtTest.GrowOnlySetValuedMapRemoteOperations.growOnlySetValuedMapRemoteOperations;
import static net.openhft.lang.MemoryUnit.MEGABYTES;
import static org.junit.Assert.assertEquals;

public class TcpReplicationSoakCrdtTest {

    static class GrowOnlySetValuedMapEntryOperations<K, E>
            implements MapEntryOperations<K, Set<E>, Void> {

        private static final GrowOnlySetValuedMapEntryOperations INSTANCE =
                new GrowOnlySetValuedMapEntryOperations();

        public static <K, E>
        MapEntryOperations<K, Set<E>, Void> growOnlySetValuedMapEntryOperations() {
            return GrowOnlySetValuedMapEntryOperations.INSTANCE;
        }

        private GrowOnlySetValuedMapEntryOperations() {}

        @Override
        public Void remove(@NotNull MapEntry<K, Set<E>> entry) {
            throw new UnsupportedOperationException("Map with grow-only set values " +
                    "doesn't support map value removals");
        }
    }

    static class GrowOnlySetValuedMapRemoteOperations<K, E>
            implements MapRemoteOperations<K, Set<E>, Void> {

        private static final GrowOnlySetValuedMapRemoteOperations INSTANCE =
                new GrowOnlySetValuedMapRemoteOperations();

        public static <K, E>
        MapRemoteOperations<K, Set<E>, Void> growOnlySetValuedMapRemoteOperations() {
            return GrowOnlySetValuedMapRemoteOperations.INSTANCE;
        }

        private GrowOnlySetValuedMapRemoteOperations() {}

        @Override
        public void put(MapRemoteQueryContext<K, Set<E>, Void> q, Data<Set<E>> newValue) {
            MapReplicableEntry<K, Set<E>> entry = q.entry();
            if (entry != null) {
                Set<E> merged = new HashSet<>(entry.value().get());
                merged.addAll(newValue.get());
                q.replaceValue(entry, q.wrapValueAsValue(merged));
            } else {
                q.insert(q.absentEntry(), newValue);
                q.entry().updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
            }
        }

        @Override
        public void remove(MapRemoteQueryContext<K, Set<E>, Void> q) {
            throw new UnsupportedOperationException();
        }
    }

    private ChronicleMap<Integer, Set<Integer>> map1;
    private ChronicleMap<Integer, Set<Integer>> map2;
    static int s_port = 8095;

    @Before
    public void setup() throws IOException {
        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        ChronicleMapBuilder<Integer, Set<Integer>> builder = ChronicleMapBuilder
                .of(Integer.class, (Class<Set<Integer>>) (Class) Set.class)
                .entries(Builder.SIZE)
                .averageValueSize(3000) // more than HashSet of 10 Integers on average, serialized
                .entryOperations(growOnlySetValuedMapEntryOperations())
                .remoteOperations(growOnlySetValuedMapRemoteOperations());

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize((int) MEGABYTES.toBytes(64));

            map1 = builder
                    .replication(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig1)
                            .name("map1")
                            .createWithId((byte) 1))
                    .instance()
                    .name("map1")
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                    (s_port + 1).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize((int) MEGABYTES.toBytes(64));

            map2 = builder
                    .replication(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig2)
                            .name("map2")
                            .createWithId((byte) 2))
                    .instance()
                    .name("map2")
                    .create();

        }
        s_port += 2;
    }


    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.gc();
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
    }


    @Test
    public void testReplicatedValuesMerged() throws InterruptedException {
        HashSet<Integer> v1 = new HashSet<>();
        v1.add(1);
        map1.put(1, v1);
        HashSet<Integer> v2 = new HashSet<>();
        v2.add(2);
        map2.put(1, v2);
        waitTillEqual(5000);
        // Map1 not equal to Map2, because it use byte-to-byte comparison of serialized HashSet
        // values, that are different for some reason
        assertEquals(map1.entrySet(), map2.entrySet());
    }

    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        try {
            System.out.print("SoakTesting ");
            for (int j = 1; j < 2 * Builder.SIZE; j++) {
                if (j % 1000 == 0)
                    System.out.print(".");
                Random rnd = new Random(j);
                for (int i = 1; i < 10; i++) {
                    final ChronicleMap<Integer, Set<Integer>> map = rnd.nextBoolean() ? map1 : map2;
                    int key = rnd.nextInt(Builder.SIZE);
                    try (ExternalMapQueryContext<Integer, Set<Integer>, ?> q =
                                 map.queryContext(key)) {
                        q.updateLock().lock();
                        MapEntry<Integer, Set<Integer>> entry = q.entry();
                        if (entry != null) {
                            Set<Integer> value = entry.value().get();
                            value.add(rnd.nextInt(20));
                            q.replaceValue(entry, q.wrapValueAsValue(value));
                        } else {
                            HashSet<Integer> value = new HashSet<>();
                            q.insert(q.absentEntry(), q.wrapValueAsValue(value));
                        }
                    }
                }
            }

            System.out.println("\nwaiting till equal");

            waitTillEqual(15000);

            // not map1.equals(map2), the reason is described above
            assertEquals(map1.entrySet(), map2.entrySet());
        } finally {
            map1.close();
            map2.close();
        }

    }


    private void waitTillEqual(final int timeOutMs) throws InterruptedException {

        Map map1UnChanged = new HashMap();
        Map map2UnChanged = new HashMap();

        int numberOfTimesTheSame = 0;
        long startTime = System.currentTimeMillis();
        for (int t = 0; t < timeOutMs + 100; t++) {
            // not map1.equals(map2), the reason is described above
            if (map1.entrySet().equals(map2.entrySet())) {
                if (map1.equals(map1UnChanged) && map2.equals(map2UnChanged)) {
                    numberOfTimesTheSame++;
                } else {
                    numberOfTimesTheSame = 0;
                    map1UnChanged = new HashMap(map1);
                    map2UnChanged = new HashMap(map2);
                }
                Thread.sleep(1);
                if (numberOfTimesTheSame == 10) {
                    System.out.println("same");
                    break;
                }

            }
            Thread.sleep(1);
            if (System.currentTimeMillis() - startTime > timeOutMs)
                break;
        }
    }
}

