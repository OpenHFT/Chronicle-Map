/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;

import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */
public class TCPSocketReplicationIntValueTest {

    static int s_port = 8010;
    private ChronicleMapBuilder<IntValue, CharSequence> map1Builder;
    private ChronicleMap<IntValue, CharSequence> map1;
    private ChronicleMap<IntValue, CharSequence> map2;
    private IntValue value;

    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);
        map1Builder = newTcpSocketShmBuilder(IntValue.class, CharSequence.class,
                (byte) 1, s_port, new InetSocketAddress("localhost", s_port + 1));
        map1 = map1Builder.create();
        map2 = newTcpSocketShmBuilder(IntValue.class, CharSequence.class,
                (byte) 2, s_port + 1)
                .entries(Builder.SIZE)
                .create();
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

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        ChannelReplicationTest.checkThreadsShutdown(threads);
    }

    @Test
    public void test3() throws IOException, InterruptedException {

        map1.put(set(5), "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    private IntValue set(int x) {
        value.setValue(x);
        return value;
    }

    // see https://higherfrequencytrading.atlassian.net/browse/HCOLL-148
    @Test
    public void test() throws IOException, InterruptedException {

        map1.put(set(1), "EXAMPLE-1");
        map1.put(set(2), "EXAMPLE-2");
        map1.put(set(3), "EXAMPLE-1");

        map2.put(set(5), "EXAMPLE-2");
        map2.put(set(6), "EXAMPLE-2");

        map1.remove(set(2));
        map2.remove(set(3));
        map1.remove(set(3));
        map2.put(set(5), "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }

    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1000; i++) {
            map1.put(set(i), "EXAMPLE-1");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map2.isEmpty());
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
        assertEquals(map1, map2);
    }


    @Test(timeout = 12000)
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        final long start = System.currentTimeMillis();
        System.out.print("SoakTesting ");
        for (int j = 1; j < 2 * Builder.SIZE; j++) {
            if (j % 1000 == 0)
                System.out.print(".");
            Random rnd = new Random(j);
            for (int i = 1; i < 10; i++) {
                final int select = rnd.nextInt(2);
                final ChronicleMap<IntValue, CharSequence> map = select > 0 ? map1 : map2;

                if (rnd.nextBoolean()) {
                    map.put(set(rnd.nextInt(Builder.SIZE)), "test");
                } else {
                    map.remove(set(rnd.nextInt(Builder.SIZE)));
                }
            }
        }

        waitTillEqual(10000);

        final long time = System.currentTimeMillis() - start;

        //assertTrue("timeTaken="+time, time < 2200);
        System.out.println("\ntime taken millis=" + time);
    }

}

