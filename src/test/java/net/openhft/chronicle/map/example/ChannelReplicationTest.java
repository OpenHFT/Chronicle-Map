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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */

public class ChannelReplicationTest {

    private ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1;
    private ChronicleMap<CharSequence, CharSequence> favoriteComputerServer2;

    private ChronicleMap<CharSequence, CharSequence> favoriteColourServer2;
    private ChronicleMap<CharSequence, CharSequence> favoriteColourServer1;

    private ReplicationHub hubOnServer1;
    private ReplicationHub hubOnServer2;

    @Test
    public void test() throws IOException, InterruptedException {

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
                    .heartBeatInterval(1, SECONDS).autoReconnectedUponDroppedConnection(true);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel1)).create();

            favoriteColourServer1.put("peter", "green");

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel2)).create();

            favoriteComputerServer1.put("peter", "dell");
        }

        // server 2 with  identifier = 2
        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(8087).heartBeatInterval(1, SECONDS)
                            .autoReconnectedUponDroppedConnection(true);

            hubOnServer2 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer2 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel1)).create();

            favoriteColourServer2.put("rob", "blue");

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer2 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel2)).create();

            favoriteComputerServer2.put("rob", "mac");
            favoriteComputerServer2.put("daniel", "mac");
        }

        // allow time for the recompilation to resolve
        for (int t = 0; t < 2500; t++) {
            if (favoriteComputerServer2.equals(favoriteComputerServer1) &&
                    favoriteColourServer2.equals(favoriteColourServer1))
                break;
            Thread.sleep(1);
        }

        assertEquals(favoriteComputerServer1, favoriteComputerServer2);
        Assert.assertEquals(3, favoriteComputerServer2.size());

        assertEquals(favoriteColourServer1, favoriteColourServer2);
        Assert.assertEquals(2, favoriteColourServer1.size());

        favoriteColourServer1.close();
        favoriteComputerServer2.close();
        favoriteColourServer2.close();
        favoriteComputerServer1.close();
    }

    @Test
    public void testPublishOnOneMapOnlyBootstrapTwice() throws IOException, InterruptedException {

        ChronicleMap<CharSequence, CharSequence> favoriteComputerServer3;

        ChronicleMap<CharSequence, CharSequence> favoriteColourServer3;

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS).autoReconnectedUponDroppedConnection(true);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel1)).create();

            favoriteColourServer1.put("peter", "green");

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel2)).create();

            favoriteComputerServer1.put("peter", "dell");
        }

        // server 2 with  identifier = 2
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 6;

            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(8087, new InetSocketAddress("localhost", 8086))
                            .heartBeatInterval(1, SECONDS)
                            .autoReconnectedUponDroppedConnection(true);

            ReplicationHub hubOnServer = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            ChronicleHashInstanceBuilder<ChronicleMap<CharSequence, CharSequence>> instance = builder.instance().replicatedViaChannel(hubOnServer.createChannel(channel1));

            favoriteColourServer2 = instance.create();
        }

        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 3;

            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(8088, new InetSocketAddress("localhost", 8086))
                            .heartBeatInterval(1, SECONDS)
                            .autoReconnectedUponDroppedConnection(true);

            ReplicationHub hubOnServer3 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer3 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer3.createChannel(channel1)).create();

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer3 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer3.createChannel(channel2)).create();
        }

        // allow time for the recompilation to resolve
        for (int t = 0; t < 250000; t++) {
            if (favoriteComputerServer1.equals(favoriteComputerServer3) &&
                    favoriteColourServer1.equals(favoriteColourServer3) &&
                    favoriteColourServer2.equals(favoriteColourServer3))
                break;
            Thread.sleep(1);
        }

        assertEquals(favoriteComputerServer3, favoriteComputerServer1);
        assertEquals(favoriteColourServer2, favoriteColourServer3);

        Assert.assertEquals(1, favoriteColourServer3.size());
        Assert.assertEquals(1, favoriteComputerServer1.size());

        favoriteComputerServer3.close();
        favoriteColourServer3.close();

        favoriteComputerServer1.close();
        favoriteColourServer1.close();

        favoriteColourServer2.close();
    }

    @Test
    public void testPublishOnOneMapOnly() throws IOException, InterruptedException {

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
                    .heartBeatInterval(1, SECONDS)
                    .autoReconnectedUponDroppedConnection(true);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel1)).create();

            favoriteColourServer1.put("peter", "green");

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer1 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel2)).create();

            favoriteComputerServer1.put("peter", "dell");
        }

        // server 2 with  identifier = 2
        {
            ChronicleMapBuilder<CharSequence, CharSequence> smallStringToStringMapBuilder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(8087).heartBeatInterval(1, SECONDS)
                            .autoReconnectedUponDroppedConnection(true);

            hubOnServer2 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer2 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel1)).create();

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer2 = smallStringToStringMapBuilder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel2)).create();
        }

        // allow time for the recompilation to resolve
        for (int t = 0; t < 2500; t++) {
            if (favoriteComputerServer2.equals(favoriteComputerServer1) &&
                    favoriteColourServer2.equals(favoriteColourServer1))
                break;
            Thread.sleep(1);
        }

        assertEquals(favoriteComputerServer1, favoriteComputerServer2);
        Assert.assertEquals(1, favoriteComputerServer2.size());

        assertEquals(favoriteColourServer1, favoriteColourServer2);
        Assert.assertEquals(1, favoriteColourServer1.size());

        favoriteColourServer1.close();
        favoriteComputerServer2.close();
        favoriteColourServer2.close();
        favoriteComputerServer1.close();
    }
}

