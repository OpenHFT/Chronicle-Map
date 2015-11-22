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

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.set.Builder;
import org.junit.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class TcpReplicationSoakTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    static int s_port = 8093;

    @Before
    public void setup() throws IOException {

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        ChronicleMapBuilder<Integer, CharSequence> builder = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(Builder.SIZE + Builder.SIZE)
                .actualSegments(1)
                .averageValue("test" + 1000);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);


            map1 = builder
                    .instance()
                    .replicated(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig1)
                            .name("map1")
                            .createWithId((byte) 1))
                    .name("map1")
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                    (s_port + 1).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map2 = builder
                    .instance()
                    .replicated(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig2)
                            .name("map2")
                            .createWithId((byte) 2))
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

    @Ignore
    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        try {
            System.out.print("SoakTesting ");
            for (int j = 1; j < 2 * Builder.SIZE; j++) {
                if (j % 1000 == 0)
                    System.out.print(".");
                Random rnd = new Random(j);
                for (int i = 1; i < 10; i++) {
                    final int select = rnd.nextInt(2);
                    final ChronicleMap<Integer, CharSequence> map = select > 0 ? map1 : map2;

                    if (rnd.nextBoolean()) {
                        map.put(rnd.nextInt(Builder.SIZE), "test" + j);
                    } else {
                        map.remove(rnd.nextInt(Builder.SIZE));
                    }
                }
            }

            System.out.println("\nwaiting till equal");

            waitTillEqual(15000);

            if (map1.equals(map2)) {
                System.out.println("same");
                return;
            }
            MapDifference<Integer, CharSequence> difference = Maps.difference(map1, map2);
            Map<Integer, CharSequence> onlyOnMap1 = difference.entriesOnlyOnLeft();
            System.out.println("only on map1:");
            for (Integer k : onlyOnMap1.keySet()) {
                printState(map1, k);
            }
            Map<Integer, CharSequence> onlyOnMap2 = difference.entriesOnlyOnRight();
            System.out.println("only on map2:");
            for (Integer k : onlyOnMap2.keySet()) {
                printState(map2, k);
            }
            if (!map1.equals(map2)) {
                Assert.assertEquals(difference.toString(), map1, map2);
            }
        } finally {
            map1.close();
            map2.close();
        }

    }

    private void printState(ChronicleMap<Integer, CharSequence> m, Integer k) {
        try (ExternalMapQueryContext<Integer, CharSequence, ?> q = m.queryContext(k)) {
            MapEntry<Integer, CharSequence> entry = q.entry();
            ReplicableEntry re = (ReplicableEntry) entry;
            System.out.println(k + "=" + entry.value() + ", isChanged=" +
                    re.isChanged() + " oTs: " + re.originTimestamp() +
                    " oId: " + re.originIdentifier());
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

