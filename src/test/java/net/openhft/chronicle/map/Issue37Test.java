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

import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class Issue37Test {

    @Test
    public void issue37Test() throws IOException {
        int port = 1000;
        String remoteHost = "localhost";
        int remotePort = 2000;

        TcpTransportAndNetworkConfig tcpReplicationConfig = TcpTransportAndNetworkConfig
                .of(port, new InetSocketAddress(remoteHost, remotePort))
                .heartBeatInterval(1, TimeUnit.SECONDS)
                .autoReconnectedUponDroppedConnection(true);

        ReplicationHub replicationHub = ReplicationHub.builder()
                .tcpTransportAndNetwork(tcpReplicationConfig)
                .maxNumberOfChannels(1024)
                .createWithId((byte)1);

        File file1 = Builder.getPersistenceFile();
        ChronicleMap<String, String> map1 = ChronicleMapBuilder.of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(2))
                .persistedTo(file1)
                .create();

        File file2 = Builder.getPersistenceFile();
        ChronicleMap<String, String> map2 = ChronicleMapBuilder.of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(3))
                .persistedTo(file2)
                .create();

        map2.close();
        map1.close();
    }
}
