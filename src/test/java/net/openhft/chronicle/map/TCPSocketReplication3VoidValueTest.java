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
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static net.openhft.chronicle.map.Builder.newMapVoid;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplication3VoidValueTest {

    private ChronicleMap<Integer, Void> map1;
    private ChronicleMap<Integer, Void> map2;
    private ChronicleMap<Integer, Void> map3;

    @Before
    public void setup() throws IOException {
        map1 = newMapVoid((byte) 1, 8036, new InetSocketAddress("localhost", 8037),
                new InetSocketAddress("localhost", 8039));
        map2 = newMapVoid((byte) 2, 8037, new InetSocketAddress("localhost", 8039));
        map3 = newMapVoid((byte) 3, 8039);
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

        assertEquals(null, map3.put(5, null));

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(15000));

        assertEquals(map3, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void test() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, null));
        assertEquals(null, map1.put(2, null));

        assertEquals(null, map2.put(5, null));
        assertEquals(null, map2.put(6, null));

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5, null);

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(5000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testClear() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, null));
        assertEquals(null, map1.put(2, null));

        assertEquals(null, map2.put(5, null));
        assertEquals(null, map2.put(6, null));

        map1.clear();

        map2.put(5, null);

        // allow time for the recompilation to resolve
        assertTrue("test timed out", waitTillEqual(15000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    private boolean waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2) &&
                    map1.equals(map3) &&
                    map2.equals(map3))
                return true;
            Thread.sleep(1);
        }
        return false;
    }

}

