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

import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

/**
 * Test ReplicatedChronicleMap where the Replicated is over a TCP Socket, but with 4 nodes
 *
 * @author Rob Austin.
 */
public class SingleMapChannelTest {

    private ChronicleMap<Integer, CharSequence> map1a;

    private ChronicleMap<Integer, CharSequence> map1b;

    private ReplicationHub hubA;
    private ReplicationHub hubB;



    @Before
    public void setup() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
                    .autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, SECONDS);

            byte identifier = 1;
            hubA = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);
            // this is how you add maps after the custer is created
            map1a = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();
        }

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8087, new InetSocketAddress("localhost", 8086))
                    .autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, SECONDS);

            hubB = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig).createWithId(
                    (byte) 2);

            // this is how you add maps after the custer is created
            map1b = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubB.createChannel((short) 1))
                    .create();
        }
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1a, map1b}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.gc();
    }

    @Test
    public void test() throws IOException, InterruptedException {

        map1a.put(1, "EXAMPLE-1");

        // allow time for the recompilation to resolve
        waitTillEqual(2500);

        Assert.assertEquals("map1a=map1b", map1a, map1b);

        assertTrue("map1a.empty", !map1a.isEmpty());
    }

    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        for (int t = 0; t < timeOutMs; t++) {
            if (map1a.equals(map1b))

                break;
            Thread.sleep(1);
        }
    }

}

