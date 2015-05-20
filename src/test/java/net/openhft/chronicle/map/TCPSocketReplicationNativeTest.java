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
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationNativeTest {

    private ChronicleMap<Integer, LongValue> map1;
    private ChronicleMap<Integer, LongValue> map2;

    @Before
    public void setup() throws IOException {
        InetSocketAddress endpoint = TcpUtil.localPort(8077);

        TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(8076, endpoint)
                .heartBeatInterval(1L, TimeUnit.SECONDS).autoReconnectedUponDroppedConnection(true);

        map1 = ChronicleMapBuilder.of(Integer.class, LongValue.class)
                .replication((byte) 1, tcpConfig1).create();

        TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of(8077)
                .heartBeatInterval(1L, TimeUnit.SECONDS).autoReconnectedUponDroppedConnection(true);

        map2 = ChronicleMapBuilder.of(Integer.class, LongValue.class)
                .replication((byte) 2, tcpConfig2).create();
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
    public void test() throws IOException, InterruptedException {

        LongValue value = DataValueClasses.newDirectReference(LongValue.class);

        try (MapKeyContext<Integer, LongValue> c = map1.acquireContext(1, value)) {
            value.setValue(10);
        }

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void test2Puts() throws IOException, InterruptedException {

        LongValue value = DataValueClasses.newDirectReference(LongValue.class);

        try (MapKeyContext<Integer, LongValue> c = map1.acquireContext(1, value)) {
            value.setValue(10);
        }

        try (MapKeyContext<Integer, LongValue> c = map1.acquireContext(1, value)) {
            value.setValue(20);
        }

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
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

