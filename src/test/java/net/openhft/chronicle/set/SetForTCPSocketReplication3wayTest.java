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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.Builder.getPersistenceFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Replicated Chronicle Set where the replication is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class SetForTCPSocketReplication3wayTest {


    private ChronicleSet<Integer> set1;
    private ChronicleSet<Integer> set2;
    private ChronicleSet<Integer> set3;


    private static <T extends ChronicleSet<Integer>> T newTcpSocketIntSet(
            final byte identifier,
            final int serverPort,
            final InetSocketAddress... endpoints) throws IOException {
        return (T) ChronicleSetBuilder.of(Integer.class)
                .replication(identifier, TcpTransportAndNetworkConfig.of(serverPort, asList(endpoints))
                        .heartBeatInterval(1L, SECONDS))
                .createPersistedTo(getPersistenceFile());
    }


    @Before
    public void setup() throws IOException {
        set1 = newTcpSocketIntSet((byte) 1, 8076, new InetSocketAddress("localhost", 8077),
                new InetSocketAddress("localhost", 8079));
        set2 = newTcpSocketIntSet((byte) 2, 8077, new InetSocketAddress("localhost", 8079));
        set3 = newTcpSocketIntSet((byte) 3, 8079);
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{set1, set2, set3}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test3() throws IOException, InterruptedException {

        set3.add(5);

        // allow time for the recompilation to resolve
        waitTillEqual(15000);

        assertEquals(set1, set2);
        assertEquals(set3, set2);
        assertTrue(!set1.isEmpty());

    }

    @Test
    public void test() throws IOException, InterruptedException {

        set1.add(1);
        set1.add(2);
        set1.add(2);

        set2.add(5);
        set2.add(6);

        set1.remove(2);
        set2.remove(3);
        set1.remove(3);
        set2.add(5);

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(set1, set2);
        assertEquals(set3, set3);
        assertTrue(!set1.isEmpty());

    }


    @Test
    @Ignore
    public void testClear() throws IOException, InterruptedException {

        set1.add(1);
        set1.add(2);
        set1.add(2);

        set2.add(5);
        set2.add(6);

        set1.clear();

        set2.add(5);

        Thread.sleep(100);

        // allow time for the recompilation to resolve
        waitTillEqual(15000);

        assertEquals(set1, set2);
        assertEquals(set3, set3);
        assertTrue(!set1.isEmpty());

    }


    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (set1.equals(set2) &&
                    set1.equals(set3) &&
                    set2.equals(set3))
                break;
            Thread.sleep(1);
        }

    }
}



