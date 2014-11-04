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


import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test ReplicatedChronicleMap where the Replicated is over a TCP Socket, but with 4 nodes
 *
 * @author Rob Austin.
 */
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
                identifier, serverPort, endpoints).create();
    }



    static ChronicleMap<IntValue, CharSequence> newTcpSocketShmIntValueString(
            final byte identifier,
            final int serverPort,
            final InetSocketAddress... endpoints) throws IOException {
        return newTcpSocketShmBuilder(IntValue.class, CharSequence.class,
                identifier, serverPort, endpoints)
                .entries(20000L)
                .keyMarshaller(ByteableIntValueMarshaller.INSTANCE).create();
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test() throws IOException, InterruptedException {
        Thread.sleep(1000);
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
        Thread.sleep(1000);
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
        Thread.sleep(1000);

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
            if (map1.equals(map2) &&
                    map1.equals(map3) &&
                    map2.equals(map3) &&
                    map4.equals(map3) &&
                    map1.equals(map4))
                break;
            Thread.sleep(1);
        }

    }

}



