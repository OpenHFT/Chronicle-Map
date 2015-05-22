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

import net.openhft.chronicle.hash.serialization.internal.DummyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import static net.openhft.chronicle.hash.serialization.internal.DummyValue.DUMMY_VALUE;
import static net.openhft.chronicle.map.Builder.newMapDummyValue;
import static net.openhft.chronicle.map.TcpUtil.localPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplication3VoidValueTest {

    private ChronicleMap<Integer, DummyValue> map1;
    private ChronicleMap<Integer, DummyValue> map2;
    private ChronicleMap<Integer, DummyValue> map3;

    @Before
    public void setup() throws IOException {
        map1 = newMapDummyValue((byte) 1, 8036, localPort(8037), localPort(8039));
        map2 = newMapDummyValue((byte) 2, 8037, localPort(8039));
        map3 = newMapDummyValue((byte) 3, 8039);
    }

    @After
    public void tearDown() throws InterruptedException {
        for (final Closeable closeable : new Closeable[]{map1, map2, map3}) {
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

        assertEquals(null, map3.put(5, DUMMY_VALUE));

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(15000));

        assertEquals(map3, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void test() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, DUMMY_VALUE));
        assertEquals(null, map1.put(2, DUMMY_VALUE));

        assertEquals(null, map2.put(5, DUMMY_VALUE));
        assertEquals(null, map2.put(6, DUMMY_VALUE));

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5, DUMMY_VALUE);

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(5000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testClear() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, DUMMY_VALUE));
        assertEquals(null, map1.put(2, DUMMY_VALUE));

        assertEquals(null, map2.put(5, DUMMY_VALUE));
        assertEquals(null, map2.put(6, DUMMY_VALUE));

        map1.clear();

        map2.put(5, DUMMY_VALUE);

        // allow time for the recompilation to resolve
        assertTrue("test timed out", waitTillEqual(15000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());
    }

    private boolean waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2) &&
                    map1.equals(map3) &&
                    map2.equals(map3))
                return true;
            Thread.sleep(1);
        }
        return false;
    }
}

