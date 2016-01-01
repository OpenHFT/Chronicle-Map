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
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class TcpTimeBasedReplicationSoakTest {

    static int s_port = 8010;
    int t = 0;
    Set<Thread> threads;
    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private IntValue value;
    private TimeProvider timeProvider;

    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);
        timeProvider = new TimeProvider() {

            Random rnd = new Random(4);

            @Override
            public long currentTimeMillis() {

                if (rnd.nextBoolean())
                    return t++;
                else
                    return t;
            }
        };

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint);

            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE)
                    .timeProvider(timeProvider)
                    .replication((byte) 1, tcpConfig1)
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of(s_port + 1);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE)
                    .timeProvider(timeProvider)
                    .replication((byte) 2, tcpConfig2)
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

    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {

        // for (int i = 30; i < 50; i++) {
        System.out.print("SoakTesting ");
        int max = 1000000;
        for (int j = 1; j < max; j++) {
            if (j % 100 == 0)
                System.out.print(".");
            Random rnd = new Random(System.currentTimeMillis());

            final ChronicleMap<Integer, CharSequence> map = rnd.nextBoolean() ? map1 : map2;

            if (rnd.nextBoolean()) {
                map.put(rnd.nextInt(100), "test" + j);
            } else {
                map.remove(rnd.nextInt(100));
            }
        }

        System.out.println("\nwaiting till equal");

        waitTillUnchanged(1000);
        System.out.println("time t=" + t);
        Assert.assertEquals(new TreeMap(map1), new TreeMap(map2));

    }

    private void waitTillUnchanged(final int timeOutMs) throws InterruptedException {

        Map map1UnChanged = new HashMap();
        Map map2UnChanged = new HashMap();

        int numberOfTimesTheSame = 0;
        for (int t = 0; t < timeOutMs + 100; t++) {

            if (map1.equals(map1UnChanged) && map2.equals(map2UnChanged)) {
                numberOfTimesTheSame++;
            } else {
                numberOfTimesTheSame = 0;
                map1UnChanged = new HashMap(map1);
                map2UnChanged = new HashMap(map2);
            }
            Thread.sleep(50);
            if (numberOfTimesTheSame == 100) {

                break;
            }
        }
    }

    private void late(TimeProvider timeProvider) {
        Mockito.when(timeProvider.currentTimeMillis()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5));
    }
}

