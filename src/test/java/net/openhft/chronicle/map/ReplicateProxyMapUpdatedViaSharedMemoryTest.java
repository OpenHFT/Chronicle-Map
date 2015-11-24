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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by khoxsey on 5/22/15.
 */
public class ReplicateProxyMapUpdatedViaSharedMemoryTest {

    @Test(timeout = 10000)
    public void testReplicateProxyMapUpdatedViaSharedMemory() throws IOException, InterruptedException {

        ChronicleMap<String, String> server1 = null;
        ChronicleMap<String, String> server2 = null;
        ChronicleMap<String, String> replicationMap = null;

        {
            final File tempFile = File.createTempFile("temp", "chron");

            server1 = ChronicleMapBuilder.of(String.class, String.class)
                    .averageKey("key").averageValue("value")
                    .entries(1)
                    .replication((byte) 1).createPersistedTo(tempFile);

            TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(7077)
                    .heartBeatInterval(500, TimeUnit.MILLISECONDS);
            replicationMap = ChronicleMapBuilder.of(String.class, String.class)
                    .entries(1)
                    .replication((byte) 1, serverConfig).createPersistedTo(tempFile);
        }

        {
            TcpTransportAndNetworkConfig server2Config =
                    TcpTransportAndNetworkConfig.of(6161, new InetSocketAddress("localhost", 7077));

            server2 = ChronicleMapBuilder.of(String.class, String.class)
                    .averageKey("key").averageValue("value")
                    .entries(1)
                    .replication((byte) 2, server2Config).create();
        }

        final String expected = "value";
        Thread.sleep(1000);
        server1.put("key", expected);

        String actual;
        do {
            actual = server2.get("key");
        } while (actual == null);

        Assert.assertEquals(expected, actual);

        server1.close();
        server2.close();
        replicationMap.close();
    }
}
