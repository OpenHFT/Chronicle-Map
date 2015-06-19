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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test ReplicatedChronicleMap where the Replicated is over a TCP Socket, but with 4 nodes
 *
 * @author Rob Austin.
 */
@Ignore
public class TCPSocketReplication4WayMapTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private ChronicleMap<Integer, CharSequence> map3;
    private ChronicleMap<Integer, CharSequence> map4;

    public static <T extends ChronicleMap<Integer, CharSequence>> T newTcpSocketShmIntString(
            final byte identifier,
            final int serverPort,
            final InetSocketAddress... endpoints) throws IOException {
        return (T) newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                identifier, serverPort, endpoints).averageValueSize(10).create();
    }

    @Before
    public void setup() throws IOException {

        map1 = newTcpSocketShmIntString((byte) 1, 8086, new InetSocketAddress("localhost", 8087),
                new InetSocketAddress("localhost", 8088), new InetSocketAddress("localhost", 8089));
        map2 = newTcpSocketShmIntString((byte) 2, 8087, new InetSocketAddress("localhost", 8088),
                new InetSocketAddress("localhost", 8089));
        map3 = newTcpSocketShmIntString((byte) 3, 8088, new InetSocketAddress("localhost", 8089));
        map4 = newTcpSocketShmIntString((byte) 4, 8089);
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2, map3, map4}) {
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
    public void test() throws IOException, InterruptedException {

        map1.put(1, "EXAMPLE-1");
        map2.put(2, "EXAMPLE-1");
        map3.put(3, "EXAMPLE-1");
        map4.remove(3);

        // allow time for the recompilation to resolve
        waitTillEqual(2500);

        assertEquals("map2", map1, map2);
        assertEquals("map3", map1, map3);
        assertEquals("map4", map1, map4);
        assertTrue("map2.empty", !map2.isEmpty());
    }

    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 50; i++) {
            map1.put(i, "EXAMPLE-1");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(15000);

        assertEquals("map2", map1, map2);
        assertEquals("map3", map1, map3);
        assertEquals("map4", map1, map4);
        assertTrue("map2.empty", !map2.isEmpty());
    }

    @Test
    public void testBufferOverflowPutIfAbsent() throws IOException, InterruptedException {


        for (int i = 0; i < 1024; i++) {
            map1.putIfAbsent(i, "EXAMPLE-1");
        }

        for (int i = 0; i < 1024; i++) {
            map1.putIfAbsent(i, "");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(10000);

        assertEquals("map2", map1, map2);
        assertEquals("map3", map1, map3);
        assertEquals("map4", map1, map4);
        assertTrue("map2.empty", !map2.isEmpty());
    }

    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if ( map1.equals(map2) &&
                            map1.equals(map3) &&
                            map1.equals(map4) &&
                            map2.equals(map3) &&
                            map2.equals(map4) &&
                            map3.equals(map4))
                break;
            Thread.sleep(1);
        }
    }

}

