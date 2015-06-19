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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class TwoMapOnDifferentServersTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {

        final TcpTransportAndNetworkConfig tcpConfig =
                TcpTransportAndNetworkConfig.of(8076, new InetSocketAddress("localhost", 8077))
                        .heartBeatInterval(1, SECONDS).autoReconnectedUponDroppedConnection(true);

        map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(20000)
                .replication((byte) 1, tcpConfig).create();

        TcpTransportAndNetworkConfig config2 = TcpTransportAndNetworkConfig.of(8077)
                .heartBeatInterval(1, SECONDS).autoReconnectedUponDroppedConnection(true);

        map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(20000)
                .replication((byte) 2, config2).create();
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
    public void test3() throws IOException, InterruptedException {

        map1.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }
    }

}