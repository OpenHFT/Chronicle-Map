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

package net.openhft.collections;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.collections.Builder.getPersistenceFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class TwoMapOnDifferentServers {


    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {

        final TcpReplicatorBuilder tcpReplicatorBuilder1 =
                new TcpReplicatorBuilder(8076, new InetSocketAddress[]{new InetSocketAddress("localhost", 8077)})
                        .heartBeatInterval(1, SECONDS);


        File persistenceFile = getPersistenceFile();
        map1 = (ChronicleMap<Integer, CharSequence>) ChronicleMapBuilder.of(Integer.class,
                CharSequence.class)
                .entries(1000)
                .identifier((byte) 1)
                .tcpReplicatorBuilder(tcpReplicatorBuilder1)
                .entries(20000).file(persistenceFile).create();

        final TcpReplicatorBuilder tcpReplicatorBuilder =
                new TcpReplicatorBuilder(8077, new InetSocketAddress[]{})
                        .heartBeatInterval(1, SECONDS);


        map2 = (ChronicleMap<Integer, CharSequence>) ChronicleMapBuilder.of(Integer.class,
                CharSequence.class)
                .entries(1000)
                .identifier((byte) 2)
                .tcpReplicatorBuilder(tcpReplicatorBuilder)
                .entries(20000).file(persistenceFile).kClass(Integer.class).vClass(CharSequence.class).create();
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test3() throws IOException, InterruptedException {

        map1.put(5, "EXAMPLE-2");

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