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

import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.set.Builder;
import net.openhft.chronicle.values.Values;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class OldDeletedEntriesCleanupTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    static int s_port = 8093;

    @Before
    public void setup() throws IOException {
        IntValue value = Values.newNativeReference(IntValue.class);
        ((Byteable) value).bytesStore(BytesStore.wrap(ByteBuffer.allocateDirect(4)), 0, 4);

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);


            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .removedEntryCleanupTimeout(1, TimeUnit.MILLISECONDS)
                    .actualSegments(1)
                    .averageValue("test" + 1000)
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
                    .tcpBufferSize(1024 * 64);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .removedEntryCleanupTimeout(1, TimeUnit.MILLISECONDS)
                    .averageValue("test" + 1000)
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
        ChannelReplicationTest.checkThreadsShutdown(threads);
    }


    @Test
    public void testFloodReplicatedMapWithDeletedEntries() throws InterruptedException {
        try {
            Random r = new Random(42);
            NavigableSet<Integer> put = new TreeSet<>();
            for (int i = 0; i < Builder.SIZE * 10; i++) {
                if (r.nextBoolean() || put.isEmpty()) {
                    int key = r.nextInt();
                    map1.put(key, "test");
                    put.add(key);
                } else {
                    Integer key = put.pollFirst();
                    map1.remove(key);
                }
//                Thread.sleep(0, 1000);
            }
            System.out.println("\nwaiting till equal");

            waitTillEqual(15000);
            if (!map1.equals(map2)) {
                Assert.assertEquals(
                        Maps.difference(new TreeMap<>(map1), new TreeMap<>(map2)).toString(),
                        new TreeMap<>(map1), new TreeMap<>(map2));
            }
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
            if (map1.equals(map2)) {
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
