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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests ReplicatedChronicleMap when the Replication is over a TCP Socket
 *
 * @author Rob Austin.
 */
public class EventListenerWithTCPSocketReplicationTest {

    ChronicleMap<Integer, CharSequence> map1;
    ChronicleMap<Integer, CharSequence> map2;

    final AtomicReference<Integer> keyRef = new AtomicReference<Integer>();

    final AtomicReference<CharSequence> replacedValueRef = new AtomicReference<CharSequence>();
    final AtomicReference<CharSequence> valueRef = new AtomicReference<CharSequence>();
    final AtomicBoolean putWasCalled = new AtomicBoolean(false);

    final AtomicBoolean wasRemoved = new AtomicBoolean(false);
    final AtomicReference<CharSequence> valueRemoved = new AtomicReference<CharSequence>();

    final MapEventListener<Integer, CharSequence> eventListener = new
            MapEventListener<Integer, CharSequence>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void onPut(Integer key, CharSequence value, CharSequence replacedValue, boolean replicationEvent) {
                    putWasCalled.getAndSet(true);
                    keyRef.set(key);
                    valueRef.set(value);
                    replacedValueRef.set(replacedValue);
                }

                @Override
                public void onRemove(Integer key, CharSequence value, boolean replicationEvent) {
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

    @Ignore
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
        waitTillReplicated(5000);

        Assert.assertEquals("Maps should be equal", map1, map2);
        Assert.assertTrue("Map 1 should not be empty", !map1.isEmpty());
        Assert.assertTrue("Map 2 should not be empty", !map2.isEmpty());

        Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        Assert.assertEquals(Integer.valueOf(5), keyRef.get());
        Assert.assertEquals("EXAMPLE", valueRef.get());
    }

    @Ignore
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
        waitTillReplicated(5000);

        Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
        Assert.assertEquals(Integer.valueOf(5), keyRef.get());
        Assert.assertEquals("EXAMPLE", valueRef.get());
        Assert.assertEquals("WILL_GET_OVER-WRITTEN", replacedValueRef.get());
    }

    @Ignore
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

        waitTillReplicated(5000);

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
        waitTillReplicated(5000);

        // no more puts after this point
        putWasCalled.set(false);

        // we will stores some data into one map here
        map1.remove(5);

        waitTillReplicated(5000);

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

    private void waitTillReplicated(final int timeOutMs) throws InterruptedException {

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
                    break;
                }

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