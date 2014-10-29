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


import net.openhft.chronicle.hash.ChannelProvider;
import net.openhft.chronicle.hash.ChannelProviderBuilder;
import net.openhft.chronicle.hash.TcpReplicationConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

/**
 * Test ReplicatedChronicleMap where the Replicated is over a TCP Socket, but with 4 nodes
 *
 * @author Rob Austin.
 */
public class SingleMapChannelTest {

    private ChronicleMap<Integer, CharSequence> map1a;

    private ChronicleMap<Integer, CharSequence> map1b;

    private ChannelProvider channelProviderA;
    private ChannelProvider channelProviderB;

    public static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/test" + System.nanoTime());
        file.deleteOnExit();
        return file;
    }


    @Before
    public void setup() throws IOException {

        {
            final TcpReplicationConfig tcpReplicationConfig = TcpReplicationConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
                    .heartBeatInterval(1, SECONDS);
            int maxEntrySize = 1024;
            byte identifier= 1;
            channelProviderA = new ChannelProviderBuilder()
                    .maxEntrySize(maxEntrySize)
                    .replicators(identifier, tcpReplicationConfig).create();
            // this is how you add maps after the custer is created
            map1a = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .channel(channelProviderA.createChannel((short) 1)).create();
        }


        {
            final TcpReplicationConfig tcpReplicationConfig = TcpReplicationConfig
                    .of(8087, new InetSocketAddress("localhost", 8086))
                    .heartBeatInterval(1, SECONDS);

            channelProviderB = new ChannelProviderBuilder()
                    .replicators((byte) 2, tcpReplicationConfig).create();

            // this is how you add maps after the custer is created
            map1b = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .channel(channelProviderB.createChannel((short) 1))
                    .entries(1000).create();
        }
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{channelProviderA, channelProviderB}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test() throws IOException, InterruptedException {

        map1a.put(1, "EXAMPLE-1");

        // allow time for the recompilation to resolve
        waitTillEqual(2500);

        Assert.assertEquals("map1a=map1b", map1a, map1b);


        assertTrue("map1a.empty", !map1a.isEmpty());


    }


    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        for (int t = 0; t < timeOutMs; t++) {
            if (map1a.equals(map1b))

                break;
            Thread.sleep(1);
        }

    }

}



