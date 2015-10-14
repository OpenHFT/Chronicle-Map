/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test  ReplicatedChronicleMap where a cluster is formed with two servers. Then when when a server
 * in the cluster is down, replication to a new server that is bootstrapping is tested.
 *
 * @author Ozan Ozen.
 */

public class TCPSocketReplicationAfterNodeFailureTest {

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
    public void testTCPSocketReplicationAfterNodeFailure()
            throws IOException, InterruptedException {

        ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1, favoriteComputerServer2, favoriteComputerServer3;
        ReplicationHub hubOnServer1, hubOnServer2, hubOnServer3;

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8121)
                    .heartBeatInterval(1, TimeUnit.SECONDS);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteComputer
            short channel1 = (short) 1;

            ReplicationChannel channel = hubOnServer1.createChannel(channel1);
            favoriteComputerServer1 = builder.instance()
                    .replicatedViaChannel(channel).create();

            favoriteComputerServer1.put("peter", "dell");
        }

        // Checkpoint for all the threads working before replicationHub on server 2 is created.
        // We would like to go back to this point in time, replication is finished between server 1
        // and server 2

        Set<Thread> threadsBeforeServer2Bootstrap = Thread.getAllStackTraces().keySet();

        // server 2 with  identifier = 2
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8122, new InetSocketAddress("localhost", 8121))
                    .heartBeatInterval(1, TimeUnit.SECONDS);

            hubOnServer2 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteComputer
            short channel1 = (short) 1;

            favoriteComputerServer2 = builder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel1)).create();

            favoriteComputerServer2.put("rob", "mac");
        }

        waitTillEqual(2500, favoriteComputerServer1, favoriteComputerServer2);


        assertEquals(favoriteComputerServer1, favoriteComputerServer2);
        assertEquals(2, favoriteComputerServer1.size());

        // Close map on server2
        favoriteComputerServer2.close();
        // Close everything on server2 as well.
        ChannelReplicationTest.checkThreadsShutdown(threadsBeforeServer2Bootstrap);

        // server 3 with  identifier = 3
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 3;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8123,
                            new InetSocketAddress("localhost", 8121),
                            new InetSocketAddress("localhost", 8122)) // this wont be available but just added for completeness
                    .heartBeatInterval(1, TimeUnit.SECONDS);

            hubOnServer3 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteComputer
            short channel1 = (short) 1;

            favoriteComputerServer3 = builder.instance()
                    .replicatedViaChannel(hubOnServer3.createChannel(channel1)).create();

            favoriteComputerServer3.put("ozan", "mac");
        }


        waitTillEqual(2500, favoriteComputerServer1, favoriteComputerServer3);


        assertEquals(favoriteComputerServer1, favoriteComputerServer3);
        assertEquals(3, favoriteComputerServer1.size());

        favoriteComputerServer1.close();
        favoriteComputerServer3.close();
    }


    private void waitTillEqual(final int timeOutMs, Map map1, Map map2) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }
    }
}

