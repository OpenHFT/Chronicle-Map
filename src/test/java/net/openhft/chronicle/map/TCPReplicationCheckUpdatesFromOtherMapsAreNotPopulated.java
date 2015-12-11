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
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPReplicationCheckUpdatesFromOtherMapsAreNotPopulated {


    static int s_port = 8093;
    Set<Thread> threads;
    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private IntValue value;

    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true).name("      map1")
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .actualSegments(1)
                    .replication((byte) 1, tcpConfig1)
                    .instance()
                    .name("map1")
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                    (s_port + 1).autoReconnectedUponDroppedConnection(true).name("map2")
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .replication((byte) 2, tcpConfig2)
                    .instance()
                    .name("map2")
                    .create();

        }
        s_port += 2;
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

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
    }

    /**
     * tests that remote puts (in otherwords, puts that provide and identifier only replicate on
     * events with with same identifier as the local-identifier
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test() throws IOException, InterruptedException {


        ReplicatedChronicleMap map = (ReplicatedChronicleMap) map1;

        map.put(1, "test", (byte) 2, System.currentTimeMillis());
        //    ((ReplicatedChronicleMap) map1).put(1, "test");
        Thread.sleep(1000);
        Assert.assertTrue(map2.isEmpty());


        map.put(1, "test2", (byte) 1, System.currentTimeMillis());
        //    ((ReplicatedChronicleMap) map1).put(1, "test");
        Thread.sleep(1000);
        Assert.assertTrue(map2.size() == 1);
        Assert.assertEquals("test2", map2.get(1));

    }


}

