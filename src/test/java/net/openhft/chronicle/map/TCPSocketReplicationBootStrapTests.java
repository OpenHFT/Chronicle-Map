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
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import static net.openhft.chronicle.map.Builder.getPersistenceFile;
import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static net.openhft.chronicle.map.TCPSocketReplication4WayMapTest.newTcpSocketShmIntString;
import static org.junit.Assert.assertEquals;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationBootStrapTests {

    private ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Test
    public void testBootstrap() throws IOException, InterruptedException {

        map1 = newTcpSocketShmIntString((byte) 1, 8079);

        ChronicleMapBuilder<Integer, CharSequence> map2aBuilder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 2, 8076, new InetSocketAddress("localhost", 8079));
        final ChronicleMap<Integer, CharSequence> map2a =
                map2aBuilder.file(getPersistenceFile()).create();
        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart map 2 but don't doConnect it to map one
            final ChronicleMap<Integer, CharSequence> map2b =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                            .file(map2File)
            .create();
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2aBuilder.file(map2File);
        map2 = map2aBuilder.create();

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));

    }


    @Test
    public void testBootstrapAndHeartbeat() throws IOException, InterruptedException {
        map1 = newTcpSocketShmIntString((byte) 1, 8079, new InetSocketAddress("localhost", 8076));
        ChronicleMapBuilder<Integer, CharSequence> map2aBuilder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class, (byte) 2, 8076);


        final ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map2a =
                (ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?>)
                        map2aBuilder.file(getPersistenceFile()).create();

        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart map 2 but don't doConnect it to map one
            ChronicleMapBuilder.of(Integer.class, CharSequence.class).file(map2File);
            final ChronicleMap<Integer, CharSequence> map2b =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class).file(map2File).
            create();
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2aBuilder.file(map2File);
        map2 = map2aBuilder.create();

        // add data into it
        waitTillEqual(20000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));


    }

    @After
    public void tearDown() {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * waits until map1 and map2 show the same value
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



