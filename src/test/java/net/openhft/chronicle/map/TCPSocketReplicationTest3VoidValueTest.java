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

import static net.openhft.chronicle.map.Builder.newMapVoid;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest3VoidValueTest {

    private ChronicleMap<Integer, Void> map1;
    private ChronicleMap<Integer, Void> map2;
    private ChronicleMap<Integer, Void> map3;

    @Before
    public void setup() throws IOException {
        map1 = newMapVoid((byte) 1, 8086, new InetSocketAddress("localhost", 8087),
                new InetSocketAddress("localhost", 8089));
        map2 = newMapVoid((byte) 2, 8087, new InetSocketAddress("localhost", 8089));
        map3 = newMapVoid((byte) 3, 8089);
    }

    @After
    public void tearDown() throws InterruptedException {
        for (final Closeable closeable : new Closeable[]{map1, map2, map3}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test3() throws IOException, InterruptedException {

        assertEquals(null, map3.put(5, null));

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(15000));

        assertEquals(map3, map2);
        assertTrue(!map1.isEmpty());

    }

    @Test
    public void test() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, null));
        assertEquals(null, map1.put(2, null));

        assertEquals(null, map2.put(5, null));
        assertEquals(null, map2.put(6,null));

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5,null);

        // allow time for the recompilation to resolve
        assertTrue(waitTillEqual(5000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());

    }


    @Test
    public void testClear() throws IOException, InterruptedException {

        assertEquals(null, map1.put(1, null));
        assertEquals(null, map1.put(2,null));

        assertEquals(null, map2.put(5, null));
        assertEquals(null, map2.put(6, null));

        map1.clear();

        map2.put(5, null);

        // allow time for the recompilation to resolve
        assertTrue("test timed out", waitTillEqual(5000));

        assertEquals(map3, map3);
        assertTrue(!map1.isEmpty());

    }

    private boolean waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2) &&
                    map1.equals(map3))
                return true;
            Thread.sleep(1);
        }
        return false;
    }

}



