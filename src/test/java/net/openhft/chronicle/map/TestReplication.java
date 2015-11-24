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

/**
 * Created by Rob Austin
 */
public class TestReplication {

    @Test
    public void testAllDataGetsReplicated() throws InterruptedException {
        TcpTransportAndNetworkConfig tcpConfigServer1 =
                TcpTransportAndNetworkConfig.of(8097);

        TcpTransportAndNetworkConfig tcpConfigServer2 =
                TcpTransportAndNetworkConfig.of(8098, new InetSocketAddress("localhost",
                        8097));

        try (ChronicleMap<Integer, Integer> map2 = ChronicleMap.of(Integer.class, Integer.class)
                .entries(70000)
                .replication((byte) 2, tcpConfigServer2)
                .create();
             ChronicleMap<Integer, Integer> map1 = ChronicleMap.of(Integer.class, Integer.class)
                     .entries(70000)
                     .replication((byte) 3, tcpConfigServer1)
                     .create()) {

            for (int i = 0; i < 70000; i++) {
                map1.put(i, i);
            }

            for (int i = 0; i < 10; i++) {
                Thread.sleep(100);
                System.out.println(map2.size());
            }

            Assert.assertEquals(map1.size(), map2.size());
        }
        System.gc();
    }

    /**
     * tests that replication works when the chronicle map that is getting the update is sharing
     * memory with the maps that is doing the update
     */
    @Test
    public void testReplicationUsingAProxyMapThatsUpdatedViaSharedMemory() throws IOException {

        ChronicleMap<String, String> server1 = null;
        ChronicleMap<String, String> forServer1Replication = null;
        ChronicleMap<String, String> server2 = null;

        // server 1  - for this test server1 and server 2 are on the same localhost but different
        // ports, to make it easier to run in a unit test
        {
            final File tempFile = File.createTempFile("test", "chron");


            server1 = ChronicleMap.of(String.class, String.class)
                    .entries(1)
                    .averageKey("key").averageValue("value")
                    .replication((byte) 1).createPersistedTo(tempFile);

            TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(9000);

            // user for replication only
            forServer1Replication = ChronicleMap.of(String.class, String.class)
                    .replication((byte) 1, serverConfig).createPersistedTo(tempFile);

        }

        // server 2- for this test server1 and server 2 are on the same localhost but different
        // ports, to make it easier to run in a unit test
        {
            TcpTransportAndNetworkConfig server2Config = TcpTransportAndNetworkConfig.of(9001, new
                    InetSocketAddress("localhost", 9000));

            server2 = ChronicleMapBuilder.of(String.class, String.class)
                    .entries(1)
                    .averageKey("key").averageValue("value")
                    .replication((byte) 2, server2Config).create();

        }

        try (ChronicleMap<String, String> s1 = server1;
             ChronicleMap<String, String> r = forServer1Replication;
             ChronicleMap<String, String> s2 = server2) {

            final String expected = "value";
            server1.put("key", expected);

            String actual;


            // we have a while loop here as we have to wait a few seconds for the data to replicate
            do {
                actual = server2.get("key");
            }
            while (actual == null);

            Assert.assertEquals(expected, actual);
        }
        System.gc();
    }
}
