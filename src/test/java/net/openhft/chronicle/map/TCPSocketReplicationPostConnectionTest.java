/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationPostConnectionTest {

    private static int s_port = 13050;
    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;

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
    public void testPostConnection() throws IOException, InterruptedException {
        int port = s_port;
        s_port += 2;
        map1 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 1, port);
        map1.put(5, "EXAMPLE-2");
        Thread.sleep(10);
        map2 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 2, port + 1,
                TcpUtil.localPort(port));

        // allow time for the recompilation to resolve
        waitTillEqual(15000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testPostConnectionNoSleep() throws IOException, InterruptedException {
        int port = s_port;
        s_port += 2;
        map1 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 1, port);
        map1.put(5, "EXAMPLE-2");
        map2 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 2, port + 1,
                TcpUtil.localPort(port));

        // allow time for the recompilation to resolve
        waitTillEqual(1000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testBootStrapIntoNewMapWithNewFile() throws IOException, InterruptedException {
        int port = s_port;
        s_port += 2;

        ChronicleMapBuilder<Integer, CharSequence> map2aBuilder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 2, port + 1, TcpUtil.localPort(port));
        try (final ChronicleMap<Integer, CharSequence> map2a =
                map2aBuilder.createPersistedTo(Builder.getPersistenceFile())) {
            map1 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 1, port);

            Thread.sleep(1);
            map1.put(5, "EXAMPLE-2");
        }

        Thread.sleep(1);
        map1.put(6, "EXAMPLE-1");

        // recreate map2 with new unique file
        map2 = map2aBuilder.createPersistedTo(Builder.getPersistenceFile());

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
        assertTrue(map2.get(6).equals("EXAMPLE-1"));
    }

    @After
    public void tearDown() {
        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.gc();
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

