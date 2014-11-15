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
import net.openhft.chronicle.java8.Function;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @author Rob Austin.
 */
public class StatelessClientTest {


    public static final int SIZE = 100000;


    static class StringBuilderToStringFunction implements Function<StringBuilder, String> {

        @Override
        public String apply(StringBuilder stringBuilder) {
            return stringBuilder.toString();
        }
    }


    @Test
    public void testMapForKeyWithEntry() throws IOException, InterruptedException {


        try (ChronicleMap<Integer, StringBuilder> serverMap = ChronicleMapBuilder.of(Integer.class,
                StringBuilder.class)
                .defaultValue(new StringBuilder())
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            serverMap.put(10, new StringBuilder("Hello World"));

            try (ChronicleMap<Integer, StringBuilder> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, StringBuilder.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {

                String actual = (String) ((StatelessChronicleMap) statelessMap).mapForKey(10, new
                        StringBuilderToStringFunction());

                Assert.assertEquals("Hello World", actual);
            }
        }
    }



    @Test
    public void testMapForKeyWhenNoEntry() throws IOException, InterruptedException {


        try (ChronicleMap<Integer, StringBuilder> serverMap = ChronicleMapBuilder.of(Integer.class,
                StringBuilder.class)
                .defaultValue(new StringBuilder())
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            serverMap.put(10, new StringBuilder("Hello World"));

            try (ChronicleMap<Integer, StringBuilder> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, StringBuilder.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {

                String actual = (String) ((StatelessChronicleMap) statelessMap).mapForKey(11, new
                        StringBuilderToStringFunction());

                Assert.assertEquals("", actual);
            }
        }
    }

    @Test(timeout = 5000)
    public void testBufferOverFlowPutAllAndEntrySet() throws IOException, InterruptedException {


        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }


                statelessMap.putAll(payload);


                Set<Map.Entry<Integer, CharSequence>> entries = statelessMap.entrySet();

                Map.Entry<Integer, CharSequence> next = entries.iterator().next();
                Assert.assertEquals("some value=" + next.getKey(), next.getValue());

                Assert.assertEquals(entries.size(), SIZE);

            }
        }
    }


    @Test
    public void testBufferOverFlowPutAllAndValues() throws IOException, InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }


                statelessMap.putAll(payload);


                Collection<CharSequence> values = statelessMap.values();


                Assert.assertEquals(values.size(), SIZE);

            }
        }
    }


    @Test
    public void testBufferOverFlowPutAllAndKeySet() throws IOException, InterruptedException {


        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }


                statelessMap.putAll(payload);


                final Set<Integer> keys = statelessMap.keySet();

                Assert.assertEquals(keys.size(), payload.size());


            }
        }
    }


    @Test
    public void test() throws IOException, InterruptedException {


        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            serverMap.put(10, "EXAMPLE-10");
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Assert.assertEquals("EXAMPLE-10", statelessMap.get(10));
                Assert.assertEquals(1, statelessMap.size());
            }
        }
    }


    @Test
    public void testClientCreatedBeforeServer() throws IOException, InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                serverMap.put(10, "EXAMPLE-10");

                Assert.assertEquals("EXAMPLE-10", statelessMap.get(10));
                Assert.assertEquals(1, statelessMap.size());

            }


        }


    }


    @Test
    public void testBufferOverFlowPutAll() throws IOException, InterruptedException {
        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }


                statelessMap.putAll(payload);

                int value = SIZE - 10;

                Assert.assertEquals("some value=" + value, statelessMap.get(value));
                Assert.assertEquals(SIZE, statelessMap.size());

            }
        }
    }


    @Test
    public void testBufferOverFlowPutAllWherePutReturnsNull() throws IOException,
            InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }


                statelessMap.putAll(payload);

                int value = SIZE - 10;

                Assert.assertEquals("some value=" + value, statelessMap.get(value));
                Assert.assertEquals(SIZE, statelessMap.size());

            }
        }

    }


    @Test
    public void testPutWherePutReturnsNull() throws IOException,
            InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {

                statelessMap.put(1, "some value");

                Assert.assertEquals("some value", statelessMap.get(1));
                Assert.assertEquals(1, statelessMap.size());


            }
        }
    }


    @Test
    public void testRemoveWhereRemoveReturnsNull() throws IOException,
            InterruptedException {

        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(8056)).create()) {

            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", 8056)).create()) {


                statelessMap.put(1, "some value");

                Assert.assertEquals("some value", statelessMap.get(1));
                Assert.assertEquals(1, statelessMap.size());

                statelessMap.remove(1);

                Assert.assertEquals(null, statelessMap.get(1));
                Assert.assertEquals(0, statelessMap.size());

            }
        }

    }

    @Test
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