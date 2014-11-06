/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.TcpReplicationConfig;
import net.openhft.lang.io.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class EventListenerTestWithTCPSocketReplication {



        @Test
        public void test() throws IOException, InterruptedException {

            ChronicleMap<Integer, CharSequence> map1;
            ChronicleMap<Integer, CharSequence> map2;
            final AtomicBoolean putWasCalled = new AtomicBoolean(false);

            int udpPort = 1234;
            MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>> eventListener = new MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void onPut(ChronicleMap<Integer, CharSequence> map, Bytes entry, int metaDataBytes, boolean added, Integer key,
                                  CharSequence value) {
                    putWasCalled.getAndSet(true);
                }
            };

            // ---------- SERVER1 1 ----------
            {

                // we connect the maps via a TCP socket connection on port 8077

                TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8076, new InetSocketAddress("localhost", 8077));
                ChronicleMapBuilder<Integer, CharSequence> map1Builder =
                        ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                                .entries(20000L)
                                .replicators((byte) 1, tcpConfig);

                map1 = map1Builder.create();
            }
            // ---------- SERVER2 2 on the same server as ----------

            {
                TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8077);
                ChronicleMapBuilder<Integer, CharSequence> map2Builder =
                        ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                                .entries(20000L)
                                .replicators((byte) 2, tcpConfig)
                                .eventListener(eventListener);

                map2 = map2Builder.create();
            }

            // we will stores some data into one map here
            map1.put(5, "EXAMPLE");
            int t = 0;
            for (; t < 5000; t++) {
                if (map1.equals(map2)) {
                    break;
                }
                Thread.sleep(1);
            }

            Assert.assertEquals("Maps should be equal", map1, map2);
            Assert.assertTrue("Map 1 should not be empty", !map1.isEmpty());
            Assert.assertTrue("Map 2 should not be empty", !map2.isEmpty());
            Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        }
    }