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
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class ReplicationTest {


    static int s_port = 8093;
    Set<Thread> threads;
    private ChronicleMap<Integer, CharSequence> map1;
    final AtomicLong count = new AtomicLong();
    final AtomicBoolean hasValueChanged = new AtomicBoolean();

    final AtomicInteger identifier = new AtomicInteger();
    final AtomicInteger replacedIdentifier = new AtomicInteger();
    final AtomicLong timeStamp = new AtomicLong();
    final AtomicLong replacedTimeStamp = new AtomicLong();

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

            final MapEventListener<Integer, CharSequence> integerCharSequenceMapEventListener =
                    new MapEventListener<Integer, CharSequence>() {
                        public void onPut(Integer key,
                                          CharSequence newValue,
                                          @Nullable CharSequence replacedValue,
                                          boolean replicationEvent,
                                          boolean added,
                                          boolean hasValueChanged,
                                          byte identifier,
                                          byte replacedIdentifier,
                                          long timeStamp,
                                          long replacedTimeStamp) {

                            System.out.println("key=" + key + ", value=" + value + ", replacedValue=" + replacedValue + ", " +
                                    "replicationEvent=" + replicationEvent + ", added=" +
                                    added + ", hasValueChanged=" + hasValueChanged + ", " +
                                    "identifier=" + identifier + ", replacedIdentifier=" +
                                    replacedIdentifier + ", timeStamp=" + timeStamp + ", " +
                                    "replacedTimeStamp=" + replacedTimeStamp);
                            count.incrementAndGet();

                            ReplicationTest.this.identifier.set(identifier);
                            ReplicationTest.this.replacedIdentifier.set(replacedIdentifier);
                            ReplicationTest.this.timeStamp.set(timeStamp);
                            ReplicationTest.this.replacedTimeStamp.set(replacedTimeStamp);

                            ReplicationTest.this.hasValueChanged.set(hasValueChanged);

                        }

                    };


            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .actualSegments(1)
                    .replication((byte) 1)
                    .eventListener(integerCharSequenceMapEventListener)
                    .instance()
                    .name("map1")
                    .create();
        }

        s_port += 2;
    }

    @After
    public void tearDown() throws InterruptedException {
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

        final long ts = 123;
        map.put(1, "test", (byte) 2, ts);
        Assert.assertTrue(hasValueChanged.get());
        Assert.assertEquals((byte) 2, identifier.get());
        Assert.assertEquals((byte) 0, replacedIdentifier.get());
        Assert.assertEquals(123, timeStamp.get());
        Assert.assertEquals((byte) 0, replacedTimeStamp.get());

        map.put(1, "test", (byte) 3, 124);
        Assert.assertFalse(hasValueChanged.get());
        Assert.assertEquals((byte) 3, identifier.get());
        Assert.assertEquals((byte) 2, replacedIdentifier.get());
        Assert.assertEquals(124, timeStamp.get());
        Assert.assertEquals(123, replacedTimeStamp.get());


        map.put(1, "test2", (byte) 2, 125);
        Assert.assertTrue(hasValueChanged.get());
        Assert.assertEquals((byte) 2, identifier.get());
        Assert.assertEquals((byte) 3, replacedIdentifier.get());
        Assert.assertEquals(125, timeStamp.get());
        Assert.assertEquals(124, replacedTimeStamp.get());

        map.put(1, "test2", (byte) 2, 125);
        Assert.assertFalse(hasValueChanged.get());
        Assert.assertEquals((byte) 2, identifier.get());
        Assert.assertEquals((byte) 2, replacedIdentifier.get());
        Assert.assertEquals(125, timeStamp.get());
        Assert.assertEquals(125, replacedTimeStamp.get());
    }


}

