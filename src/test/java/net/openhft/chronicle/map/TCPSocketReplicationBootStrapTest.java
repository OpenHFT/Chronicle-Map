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

import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.set.Builder.getPersistenceFile;
import static net.openhft.chronicle.set.Builder.newTcpSocketShmBuilder;
import static net.openhft.chronicle.map.TCPSocketReplication4WayMapTest.newTcpSocketShmIntString;
import static org.junit.Assert.assertEquals;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */
public class TCPSocketReplicationBootStrapTest {

    private ReplicatedChronicleMap<Integer, CharSequence, ?> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Test
    public void testBootstrap() throws IOException, InterruptedException {

        map1 = newTcpSocketShmIntString((byte) 1, 8091);

        ChronicleMapBuilder<Integer, CharSequence> map2aBuilder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 2, 8092, new InetSocketAddress("localhost", 8091))
                .averageValue("EXAMPLE-10");
        File persistenceFile = getPersistenceFile();
        final ChronicleMap<Integer, CharSequence> map2a =
                map2aBuilder.createPersistedTo(persistenceFile);
        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart map 2 but don't doConnect it to map one
            final ChronicleMap<Integer, CharSequence> map2b =
                    map2aBuilder.createPersistedTo(map2File);
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2 = map2aBuilder.createPersistedTo(map2File);

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));

        map2.file().delete();
        persistenceFile.delete();
    }

    @Test
    public void testBootstrapAndHeartbeat() throws IOException, InterruptedException {

        TcpTransportAndNetworkConfig map1Config = TcpTransportAndNetworkConfig
                .of(8068, Arrays.asList(new InetSocketAddress("localhost", 8067)))
                .heartBeatInterval(1L, TimeUnit.SECONDS)
                .autoReconnectedUponDroppedConnection(true);

        map1 = (ReplicatedChronicleMap<Integer, CharSequence, ?>)
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .averageValueSize(100)
                .replication(SingleChronicleHashReplication.builder()
                        .tcpTransportAndNetwork(map1Config)
                        .name("map1")
                        .createWithId((byte) 1))
                .create();

        File persistenceFile = getPersistenceFile();

        TcpTransportAndNetworkConfig map2Config = TcpTransportAndNetworkConfig.of(8067)
                .heartBeatInterval(1L, TimeUnit.SECONDS);

        final ReplicatedChronicleMap<Integer, CharSequence, ?> map2a =
                (ReplicatedChronicleMap<Integer, CharSequence, ?>)
                        ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                                .averageValueSize(100)
                                .replication(SingleChronicleHashReplication.builder()
                                        .tcpTransportAndNetwork(map2Config)
                                        .name("map2")
                                        .createWithId((byte) 2))
                                .createPersistedTo(persistenceFile);

        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        map2a.close();

        {
            // restart map 2 but does not connect it to map1
            final ChronicleMap<Integer, CharSequence> map2b = ChronicleMap
                    .of(Integer.class, CharSequence.class).averageValueSize(100)
                    .replication((byte) 2).createPersistedTo(persistenceFile);
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        TcpTransportAndNetworkConfig tcpConfigNewMap2 = TcpTransportAndNetworkConfig.of(8067)
                .heartBeatInterval(1L, TimeUnit.SECONDS);
        map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .averageValueSize(100)
                .replication(SingleChronicleHashReplication.builder()
                        .tcpTransportAndNetwork(tcpConfigNewMap2)
                        .name("newMap2")
                        .createWithId((byte) 2))
                .createPersistedTo(persistenceFile);

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));
    }

    @After
    public void tearDown() {
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

    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
            if (System.currentTimeMillis() - startTime > timeOutMs)
                break;
        }
    }

}

