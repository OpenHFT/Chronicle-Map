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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static net.openhft.chronicle.map.TCPSocketReplication4WayMapTest.newTcpSocketShmIntString;
import static org.junit.Assert.*;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */
public class TCPSocketReplication3wayTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private ChronicleMap<Integer, CharSequence> map3;

    @Before
    public void setup() throws IOException {
        map1 = newTcpSocketShmIntString((byte) 1, 8026, new InetSocketAddress("localhost", 8027),
                new InetSocketAddress("localhost", 8029));
        map2 = newTcpSocketShmIntString((byte) 2, 8027, new InetSocketAddress("localhost", 8029));
        map3 = newTcpSocketShmIntString((byte) 3, 8029);
    }

    @After
    public void tearDown() throws InterruptedException {
        for (final Closeable closeable : new Closeable[]{map1, map2, map3}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    @Test
    public void test3() throws IOException, InterruptedException {

        assertEquals(null, map3.put(5, "EXAMPLE-2"));

        // allow time for the recompilation to resolve
        waitTillEqual(15000);

        assertEquals(map1, map2);
        assertEquals(map3, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void test() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, "EXAMPLE-1"));
        assertEquals(null, map1.put(2, "EXAMPLE-2"));
        assertNotEquals(null, map1.put(2, "EXAMPLE-1"));

        assertEquals(null, map2.put(5, "EXAMPLE-2"));
        assertEquals(null, map2.put(6, "EXAMPLE-2"));

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testClear() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, "EXAMPLE-1"));
        assertEquals(null, map1.put(2, "EXAMPLE-2"));
        assertNotEquals(null, map1.put(2, "EXAMPLE-1"));

        assertEquals(null, map2.put(5, "EXAMPLE-2"));
        assertEquals(null, map2.put(6, "EXAMPLE-2"));

        map1.clear();

        map2.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testPutIfAbsent() throws IOException, InterruptedException {

        assertEquals(null, map1.putIfAbsent(1, "EXAMPLE-1"));
        assertNotEquals(null, map1.putIfAbsent(1, "EXAMPLE-2"));
        assertEquals(null, map1.putIfAbsent(2, "EXAMPLE-2"));
        assertEquals(null, map1.putIfAbsent(3, "EXAMPLE-1"));

        assertEquals(null, map2.putIfAbsent(5, "EXAMPLE-2"));
        assertEquals(null, map2.putIfAbsent(6, "EXAMPLE-2"));

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.putIfAbsent(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2) &&
                    map1.equals(map3)
                    && map2.equals(map3))
                break;
            Thread.sleep(1);
        }
    }
}

