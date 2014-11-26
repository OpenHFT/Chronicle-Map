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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.lang.io.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */
public class EventListenerTestWithTCPSocketReplication {


    ChronicleMap<Integer, CharSequence> map1;
    ChronicleMap<Integer, CharSequence> map2;

    final AtomicReference<Integer> keyRef = new AtomicReference<Integer>();

    final AtomicReference<CharSequence> replacedValueRef = new AtomicReference<CharSequence>();
    final AtomicReference<CharSequence> valueRef = new AtomicReference<CharSequence>();
    final AtomicBoolean putWasCalled = new AtomicBoolean(false);

    final AtomicBoolean wasRemoved = new AtomicBoolean(false);
    final AtomicReference<CharSequence> valueRemoved = new AtomicReference<CharSequence>();

    final MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>> eventListener = new
            MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void onPut(ChronicleMap<Integer, CharSequence> map, Bytes entry, int metaDataBytes, boolean added, Integer key,
                                  CharSequence value, CharSequence replacedValue) {
                    putWasCalled.getAndSet(true);
                    keyRef.set(key);
                    valueRef.set(value);
                    replacedValueRef.set(replacedValue);
                }

                @Override
                public void onRemove(ChronicleMap<Integer, CharSequence> map, Bytes entry, int metaDataBytes, Integer key, CharSequence value) {
                    wasRemoved.set(true);
                    valueRemoved.set(value);
                }
            };


    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        wasRemoved.set(false);
        valueRemoved.set(null);
        putWasCalled.set(false);
        valueRef.set(null);
        replacedValueRef.set(null);
        keyRef.set(null);
        System.gc();
    }


    @Test
    public void testAdded() throws IOException, InterruptedException {


        // ---------- SERVER1 1 ----------
        {

            // we connect the maps via a TCP socket connection on port 8077

            ChronicleMapBuilder<Integer, CharSequence> map1Builder = serverBuilder();

            map1 = map1Builder.create();
        }
        // ---------- SERVER2 2 on the same server as ----------

        {
            ChronicleMapBuilder<Integer, CharSequence> map2Builder = clientBuilder();

            map2 = map2Builder.create();
        }

        // we will stores some data into one map here
        map1.put(5, "EXAMPLE");
        waitTillReplicated();

        Assert.assertEquals("Maps should be equal", map1, map2);
        Assert.assertTrue("Map 1 should not be empty", !map1.isEmpty());
        Assert.assertTrue("Map 2 should not be empty", !map2.isEmpty());

        Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        Assert.assertEquals(Integer.valueOf(5), keyRef.get());
        Assert.assertEquals("EXAMPLE", valueRef.get());


    }


    @Test
    public void testRemoteNotifyPutLargerValue() throws IOException, InterruptedException {

        // ---------- SERVER1 1 ----------
        {

            // we connect the maps via a TCP socket connection on port 8077

            ChronicleMapBuilder<Integer, CharSequence> map1Builder = serverBuilder();

            map1 = map1Builder.create();


        }
        // ---------- SERVER2 2 on the same server as ----------

        {

            map2 = clientBuilder().create();
            map2.put(5, "WILL_GET_OVER-WRITTEN");
        }


        // we will stores some data into one map here
        map1.put(5, "EXAMPLE");
        waitTillReplicated();


        Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        Assert.assertEquals(Integer.valueOf(5), keyRef.get());
        Assert.assertEquals("EXAMPLE", valueRef.get());
        Assert.assertEquals("WILL_GET_OVER-WRITTEN", replacedValueRef.get());

    }


    @Test
    public void testRemoteNotifyPutSmallerValue() throws IOException, InterruptedException {


        // ---------- SERVER1 1 ----------
        {

            // we connect the maps via a TCP socket connection on port 8077

            map1 = serverBuilder().create();

        }
        // ---------- SERVER2 2 on the same server as ----------

        {

            map2 = clientBuilder().create();
            map2.put(5, "small");
        }


        // we will stores some data into one map here
        map1.put(5, "EXAMPLE");

        waitTillReplicated();


        Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        Assert.assertEquals(Integer.valueOf(5), keyRef.get());
        Assert.assertEquals("small", replacedValueRef.get());
        Assert.assertEquals("EXAMPLE", valueRef.get());

    }


    @Test
    public void testRemoteNotifyRemove() throws IOException, InterruptedException {

        // ---------- SERVER1 1 ----------
        {

            // we connect the maps via a TCP socket connection on port 8077

            ChronicleMapBuilder<Integer, CharSequence> map1Builder = serverBuilder();

            map1 = map1Builder.create();


        }

        // ---------- SERVER2 2 on the same server as ----------
        {
            map2 = clientBuilder().create();
            map2.put(5, "WILL_GET_REMOVED");
        }

        // its we have to wait here for this entry to be removed - as removing an entry that you dont have
        // on map 1 will have no effect
        waitTillReplicated();

        // no more puts after this point
        putWasCalled.set(false);

        // we will stores some data into one map here
        map1.remove(5);

        waitTillReplicated();

        Assert.assertTrue(!putWasCalled.get());
        Assert.assertTrue(wasRemoved.get());
        Assert.assertEquals("WILL_GET_REMOVED", valueRemoved.get());
        Assert.assertEquals(null, map2.get(5));
    }

    private ChronicleMapBuilder<Integer, CharSequence> clientBuilder() {
        TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig.of(8077)
                .autoReconnectedUponDroppedConnection(true);
        return ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(20000L)
                .replication((byte) 1, tcpConfig)
                .eventListener(eventListener);
    }


    private void waitTillReplicated() throws InterruptedException {
        int t = 0;
        for (; t < 5000; t++) {
            if (map1.equals(map2)) {
                break;
            }
            Thread.sleep(1);
        }
    }

    private ChronicleMapBuilder<Integer, CharSequence> serverBuilder() {
        TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig.of(8076, new InetSocketAddress("localhost", 8077))
                .autoReconnectedUponDroppedConnection(true);
        return ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(20000L)
                .replication((byte) 2, tcpConfig);
    }

}