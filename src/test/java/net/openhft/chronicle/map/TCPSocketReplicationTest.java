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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest {

    static int s_port = 12050;
    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {
        int port = s_port;
        ChronicleMapBuilder<Integer, CharSequence> map1Builder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 1, port, new InetSocketAddress("localhost", port + 1));
        map1 = map1Builder.entries(Builder.SIZE).averageValueSize(10).create();

        ChronicleMapBuilder<Integer, CharSequence> map2Builder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 2, port + 1);
        map2 = map2Builder.entries(Builder.SIZE).averageValueSize(10).create();
        s_port += 2;
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

        Thread.sleep(9999999);
    }

    @Test
    public void test() throws IOException, InterruptedException {

        map1.put(1, "EXAMPLE-1");
        map1.put(2, "EXAMPLE-2");
        map1.put(3, "EXAMPLE-1");

        map2.put(5, "EXAMPLE-2");
        map2.put(6, "EXAMPLE-2");

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }


    @Test
    public void test2() throws IOException, InterruptedException {

        map2.put(1, "EXAMPLE-1");
        map2.remove(1);

        map1.put(1, "EXAMPLE-2");
        map1.remove(1);

        map2.put(1, "EXAMPLE-1");


        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void test4() throws IOException, InterruptedException {

        map2.put(1, "EXAMPLE-1");
        map2.remove(1);

        map1.put(1, "EXAMPLE-2");
        map1.remove(1);

        map2.put(1, "EXAMPLE-1");
        map2.remove(1);

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(map1.isEmpty());
    }


    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1000; i++) {
            map1.put(i, "EXAMPLE-1");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(9000);

        assertEquals(map1, map2);
        assertTrue(!map2.isEmpty());
    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private void waitTillEqual(final int timeOutMs) throws InterruptedException {

        Map map1UnChanged = new HashMap();
        Map map2UnChanged = new HashMap();

        int numberOfTimesTheSame = 0;
        for (int t = 0; t < timeOutMs + 100; t++) {
            if (map1.equals(map2)) {
                if (map1.equals(map1UnChanged) && map2.equals(map2UnChanged)) {
                    numberOfTimesTheSame++;
                } else {
                    numberOfTimesTheSame = 0;
                    map1UnChanged = new HashMap(map1);
                    map2UnChanged = new HashMap(map2);
                }
                Thread.sleep(1);
                if (numberOfTimesTheSame == 100) {
                    System.out.println("same");
                    break;
                }

            }
            Thread.sleep(1);
        }
    }

    // TODO test this with larger sizes.
    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        System.out.print("SoakTesting ");
        for (int j = 1; j < Builder.SIZE * 2; j++) {
            if (j % 1000 == 0)
                System.out.print(".");
            Random rnd = new Random(j);
            for (int i = 1; i < 10; i++) {
                final int select = rnd.nextInt(2);
                final ChronicleMap<Integer, CharSequence> map = select > 0 ? map1 : map2;

                switch (rnd.nextInt(2)) {
                    case 0:
                        map.put(rnd.nextInt(Builder.SIZE), "test");
                        break;
                    case 1:
                        map.remove(rnd.nextInt(Builder.SIZE));
                        break;
                }
            }
        }
        System.out.println("");
        waitTillEqual(1000);
    }

}

