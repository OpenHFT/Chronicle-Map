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

import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.*;

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
@Ignore
public class PostChannelCreationTest {

    private ChronicleMap<Integer, CharSequence> map1a;
    private ChronicleMap<Integer, CharSequence> map2a;

    private ChronicleMap<Integer, CharSequence> map1b;
    private ChronicleMap<Integer, CharSequence> map2b;

    private ReplicationHub hubA;
    private ReplicationHub hubB;

    public static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/test" + System.nanoTime());
        file.deleteOnExit();
        return file;
    }

    @Before
    public void setup() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            map2a = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();
        }

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8087, new InetSocketAddress("localhost", 8086))
                    .heartBeatInterval(1, SECONDS);

            hubB = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 2);

            // this is how you add maps after the custer is created
            map1b = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubB.createChannel((short) 1)).create();

            map2b = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubB.createChannel((short) 2)).create();
        }
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1a, map1b, map2a, map2b}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.gc();
    }

    /**
     * sets up a 2 clusters ( notionally one for each host ), and replicates 2 Chronicle Maps between them
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test() throws IOException, InterruptedException {

        map2a.put(1, "EXAMPLE-2");
        map1a.put(1, "EXAMPLE-1");

        // allow time for the recompilation to resolve
        waitTillEqual(2500);

        Assert.assertEquals("map1a=map1b", map1a, map1b);
        Assert.assertEquals("map2a=map2b", map2a, map2b);

        assertTrue("map1a.empty", !map1a.isEmpty());
        assertTrue("map2a.empty", !map2a.isEmpty());
    }

    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        for (int t = 0; t < timeOutMs; t++) {
            if (map1a.equals(map1b) &&
                    map2a.equals(map2b))
                break;
            Thread.sleep(1);
        }
    }

}

