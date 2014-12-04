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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class StatelessClientTest {

    public static final int SIZE = 2500;
    static int s_port = 9050;

    enum ToString implements Function<Object, String> {
        INSTANCE;

        @Override
        public String apply(Object obj) {
            return obj.toString();
        }
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        checkThreadsShutdown(threads);
    }

    public static void checkThreadsShutdown(Set<Thread> threads) {
        // give them a change to stop if there were killed.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.keySet().removeAll(threads);
        if (!threadMap.isEmpty()) {
            System.out.println("### threads still running after the test ###");
            for (Map.Entry<Thread, StackTraceElement[]> entry : threadMap.entrySet()) {
                System.out.println(entry.getKey());
                for (StackTraceElement ste : entry.getValue()) {
                    System.out.println("\t" + ste);
                }
            }
            try {
                for (Thread thread : threadMap.keySet()) {
                    if (thread.isAlive()) {
                        System.out.println("Waiting for " + thread);
                        thread.join(1000);
                        if (thread.isAlive()) {
                            System.out.println("Forcing " + thread + " to die");
                            thread.stop();
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test(timeout = 10000)
    public void testMapForKeyWithEntry() throws IOException, InterruptedException {
        int port = s_port++;
        try (ChronicleMap<Integer, StringBuilder> serverMap = ChronicleMapBuilder
                .of(Integer.class, StringBuilder.class)
                .putReturnsNull(true)
                .entries(SIZE)
                .defaultValue(new StringBuilder())
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            serverMap.put(10, new StringBuilder("Hello World"));

            try (ChronicleMap<Integer, StringBuilder> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, StringBuilder.class)
                    .putReturnsNull(true)
                    .statelessClient(new InetSocketAddress("localhost", port)).create()) {
                String actual = statelessMap.mapForKey(10, ToString.INSTANCE);

                assertEquals("Hello World", actual);
            }
        }
    }

    @Test(timeout = 10000)
    public void testMapForKeyWhenNoEntry() throws IOException, InterruptedException {
        int port = s_port++;
        try (ChronicleMap<Integer, StringBuilder> serverMap = ChronicleMapBuilder
                .of(Integer.class, StringBuilder.class)
                .defaultValue(new StringBuilder())
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port)).create()) {
            serverMap.put(10, new StringBuilder("Hello World"));

            try (ChronicleMap<Integer, StringBuilder> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, StringBuilder.class)
                    .statelessClient(new InetSocketAddress("localhost", port)).create()) {
                String actual = statelessMap.mapForKey(11, ToString.INSTANCE);

                assertEquals(null, actual);
            }
        }
    }

    @Test(timeout = 10000)
    public void testBufferOverFlowPutAllAndEntrySet() throws IOException, InterruptedException {
        int port = s_port++;
        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                Set<Map.Entry<Integer, CharSequence>> entries = statelessMap.entrySet();

                Map.Entry<Integer, CharSequence> next = entries.iterator().next();
                assertEquals("some value=" + next.getKey(), next.getValue());

                assertEquals(SIZE, entries.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testBufferOverFlowPutAllAndValues() throws IOException, InterruptedException {
        int port = s_port++;

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                Collection<CharSequence> values = statelessMap.values();

                assertEquals(values.size(), SIZE);
            }
        }
    }

    @Test(timeout = 10000)
    public void testBufferOverFlowPutAllAndKeySet() throws IOException, InterruptedException {
        int port = s_port++;

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                final Set<Integer> keys = statelessMap.keySet();

                assertEquals(keys.size(), payload.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void test() throws IOException, InterruptedException {
        int port = s_port++;

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            serverMap.put(10, "EXAMPLE-10");
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port)).create()) {
                assertEquals("EXAMPLE-10", statelessMap.get(10));

         int size = statelessMap.size();
                assertEquals(1, size);
            }
        }
    }

    @Test(timeout = 10000)
    public void testClientCreatedBeforeServer() throws IOException, InterruptedException {
        int port = s_port++;

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port)).create()) {
                serverMap.put(10, "EXAMPLE-10");

                assertEquals("EXAMPLE-10", statelessMap.get(10));
                assertEquals(1, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testServerPutStringKeyMap() throws IOException, InterruptedException {

        try (ChronicleMap<String, Map> serverMap = ChronicleMapBuilder
                .of(String.class, Map.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056))
                .create()) {
            try (ChronicleMap<String, Map> statelessMap = ChronicleMapBuilder
                    .of(String.class, Map.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056))
                    .create()) {
                serverMap.put("hello", Collections.singletonMap("hello", "world"));

                assertEquals(Collections.singletonMap("hello", "world"), statelessMap.get("hello"));
                assertEquals(1, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testStringKeyMapPutIntoStatelessMap() throws IOException, InterruptedException {

        final Map<String, String> data = new HashMap<String, String>();

        String value = new String(new char[10]);
        for (int i = 0; i < 1000; i++) {
            data.put("" + i, value);
        }

        try (ChronicleMap<String, Map> serverMap = ChronicleMapBuilder
                .of(String.class, Map.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056))
                .create()) {
            try (ChronicleMap<String, Map> statelessMap = ChronicleMapBuilder
                    .of(String.class, Map.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056))
                    .create()) {
                statelessMap.put("hello", data);

                assertEquals(data, serverMap.get("hello"));
                assertEquals(1, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testBufferOverFlowPutAll() throws IOException, InterruptedException {
        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                int value = SIZE - 10;

                assertEquals("some value=" + value, statelessMap.get(value));
                assertEquals(SIZE, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testBufferOverFlowPutAllWherePutReturnsNull() throws IOException, InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                int value = SIZE - 10;

                assertEquals("some value=" + value, statelessMap.get(value));
                assertEquals(SIZE, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testPutWherePutReturnsNull() throws IOException,
            InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                statelessMap.put(1, "some value");

                assertEquals("some value", statelessMap.get(1));
                assertEquals(1, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testRemoveWhereRemoveReturnsNull() throws IOException,
            InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                statelessMap.put(1, "some value");

                assertEquals("some value", statelessMap.get(1));
                assertEquals(1, statelessMap.size());

                statelessMap.remove(1);

                assertEquals(null, statelessMap.get(1));
                assertEquals(0, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testGetLater() throws IOException,
            InterruptedException, ExecutionException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                statelessMap.put(1, "some value");

                assertEquals("some value", statelessMap.getLater(1).get());
                assertEquals(1, statelessMap.size());

                statelessMap.remove(1);

                assertEquals(null, statelessMap.getLater(1).get());
                assertEquals(0, statelessMap.size());
            }
        }
    }

    @Test
    public void testPutLater() throws IOException,
            InterruptedException, ExecutionException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                CharSequence oldValue = statelessMap.putLater(1, "some value").get();
                assertEquals("some value", statelessMap.get(1));
                assertEquals(1, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testRemoveLater() throws IOException,
            InterruptedException, ExecutionException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {
                statelessMap.put(1, "some value");
                statelessMap.removeLater(1).get();
                assertEquals(null, statelessMap.get(1));
                assertEquals(0, statelessMap.size());
            }
        }
    }

    @Test(timeout = 10000)
    public void testEquals() throws IOException, InterruptedException {

        final ChronicleMap<Integer, CharSequence> serverMap1;
        final ChronicleMap<Integer, CharSequence> serverMap2;
        final ChronicleMap<Integer, CharSequence> statelessMap1;
        final ChronicleMap<Integer, CharSequence> statelessMap2;

        // server
        {
            serverMap1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create();
            serverMap2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replication((byte) 2, TcpTransportAndNetworkConfig.of(8077)).create();
        }

        // stateless client
        {
            statelessMap1 = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create();
            statelessMap2 = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8077)).create();
        }

        Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();
        for (int i = 0; i < 1000; i++) {
            payload.put(i, "some value=" + i);
        }

        statelessMap1.putAll(payload);
        Assert.assertTrue(statelessMap1.equals(payload));

        statelessMap1.close();
        statelessMap2.close();

        serverMap1.close();
        serverMap2.close();
    }
}