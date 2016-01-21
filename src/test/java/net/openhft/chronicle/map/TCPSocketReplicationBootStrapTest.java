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

import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.Builder.getPersistenceFile;
import static net.openhft.chronicle.map.Builder.newTcpSocketShmBuilder;
import static net.openhft.chronicle.map.TCPSocketReplication4WayMapTest.newTcpSocketShmIntString;
import static org.junit.Assert.assertEquals;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationBootStrapTest {

    private ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    @Test
    public void testBootstrap() throws IOException, InterruptedException {

        map1 = newTcpSocketShmIntString((byte) 1, 8091);

        ChronicleMapBuilder<Integer, CharSequence> map2aBuilder =
                newTcpSocketShmBuilder(Integer.class, CharSequence.class,
                        (byte) 2, 8092, new InetSocketAddress("localhost", 8091));
        File persistenceFile = getPersistenceFile();
        final ChronicleMap<Integer, CharSequence> map2a =
                map2aBuilder.createPersistedTo(persistenceFile);
        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to chronicleMap 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart chronicleMap 2 but don't doConnect it to chronicleMap one
            final ChronicleMap<Integer, CharSequence> map2b =
                    map2aBuilder.createPersistedTo(map2File);
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2 = map2aBuilder.createPersistedTo(map2File);

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));

        map2.file().delete();
        persistenceFile.delete();
    }

    @Test
    public void testReplication() throws IOException, InterruptedException {

        SingleChronicleHashReplication replicationHubForChannelIdMap =
                SingleChronicleHashReplication.builder().bootstrapOnlyLocalEntries(true)
                        .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5085, new InetSocketAddress[0]))
                        .createWithId((byte) 1);
        replicationHubForChannelIdMap.bootstrapOnlyLocalEntries();

        ChronicleMap<java.lang.String, Integer> channelIdMap = ChronicleMapBuilder.of(java.lang.String.class, Integer.class)
                .entries(Short.MAX_VALUE)
                .instance()
                .replicated(replicationHubForChannelIdMap)
                .create();


        channelIdMap.put("1", 1);
        SingleChronicleHashReplication replicationHubForChannelIdMap2 =
                SingleChronicleHashReplication.builder().bootstrapOnlyLocalEntries(true)
                        .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5086, new InetSocketAddress("localhost", 5085)))
                        .createWithId((byte) 2);
        replicationHubForChannelIdMap.bootstrapOnlyLocalEntries();

        ChronicleMap<java.lang.String, Integer> channelIdMap2 = ChronicleMapBuilder.of(java.lang.String.class, Integer.class)
                .entries(Short.MAX_VALUE)
                .instance()
                .replicated(replicationHubForChannelIdMap2)
                .create();


        Thread.sleep(300);

        int map2Size = channelIdMap2.size();
        channelIdMap.close();
        channelIdMap2.close();
        assertEquals(1, map2Size);
    }

    @Test
    public void testReplicationWhileModifying() throws IOException, InterruptedException {

        SingleChronicleHashReplication replicationHubForChannelIdMap =
                SingleChronicleHashReplication.builder().bootstrapOnlyLocalEntries(true)
                        .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5085, new InetSocketAddress[0]))
                        .createWithId((byte) 1);

        ChronicleMap<java.lang.String, Integer> channelIdMap = ChronicleMapBuilder.of(java.lang.String.class, Integer.class)
                .entries(Short.MAX_VALUE)
                .instance()
                .replicated(replicationHubForChannelIdMap)
                .create();

        int i;
        for (i = 0; i < 500; i++) {
            channelIdMap.put(Integer.toString(i), i);
        }
        MapReader reader = new MapReader();
        Thread t = new Thread(reader);
        t.start();
        int j = 0;
        while (j < 100) {
            i = 400;
            while (i < 500) {
                channelIdMap.update(Integer.toString(i), i + 1);
                i++;
            }
            j++;
        }
        reader.stop();
        channelIdMap.close();
        int firstGatheredSize = reader.getFirstGatheredSize();
        assertEquals(0, reader.getFirstGatheredElement());
        assertEquals(500, firstGatheredSize);
    }

    @Test
    public void testReplicationWhileModifyingWithMapListener() throws IOException, InterruptedException {

        SingleChronicleHashReplication replicationHubForChannelIdMap =
                SingleChronicleHashReplication.builder()
                        .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5085, new InetSocketAddress[0]))
                        .createWithId((byte) 1);

        ChronicleMap<java.lang.String, Integer> channelIdMap = ChronicleMapBuilder.of(java.lang.String.class, Integer.class)
                .entries(Short.MAX_VALUE)
                .instance()
                .replicated(replicationHubForChannelIdMap)
                .create();

        int i;
        for (i = 0; i < 500; i++) {
            channelIdMap.put(Integer.toString(i), i);
        }
        MapReaderWithListener reader = new MapReaderWithListener();
        Thread t = new Thread(reader);
        t.start();
        int j = 0;
        while (j < 15000) {
            i = 400;
            while (i < 500) {
                channelIdMap.update(Integer.toString(i), i + 1);
                i++;
            }
            Thread.sleep(1);
            j++;
        }
        reader.stop();
        channelIdMap.close();
        assertEquals(0, reader.getFirstGatheredElement());
    }

    @Test
    public void testBootstrapAndHeartbeat() throws IOException, InterruptedException {

        TcpTransportAndNetworkConfig map1Config = TcpTransportAndNetworkConfig.of(8068, Arrays.asList(new InetSocketAddress("localhost", 8067)))
                .heartBeatInterval(1L, TimeUnit.SECONDS).name("map1").autoReconnectedUponDroppedConnection
                        (true);

        map1 = (ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?>) ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 1, map1Config).create();

        File persistenceFile = getPersistenceFile();

        TcpTransportAndNetworkConfig map2Config = TcpTransportAndNetworkConfig.of(8067)
                .heartBeatInterval(1L, TimeUnit.SECONDS).name("map2");

        final ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map2a =
                (ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?>)
                        ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                                .replication((byte) 2, map2Config).createPersistedTo(persistenceFile);

        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to chronicleMap 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        map2a.close();

        {
            // restart chronicleMap 2 but does not connect it to map1
            final ChronicleMap<Integer, CharSequence> map2b = ChronicleMapBuilder.of(Integer.class,
                    CharSequence.class).replication((byte) 2).createPersistedTo(persistenceFile);
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        TcpTransportAndNetworkConfig tcpConfigNewMap2 = TcpTransportAndNetworkConfig.of(8067)
                .heartBeatInterval(1L, TimeUnit.SECONDS).name("newMap2");
        map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .replication((byte) 2, tcpConfigNewMap2).createPersistedTo(persistenceFile);

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));
    }

    @After
    public void tearDown() {
        if (map1 != null && map2 != null) {
            for (final Closeable closeable : new Closeable[]{map1, map2}) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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

    private class MapReader implements Runnable {

        private volatile ChronicleMap<java.lang.String, Integer> chronicleMap;
        private boolean stop = false;
        private int firstGatheredSize = 0;
        private Object firstGatheredElement;

        public MapReader() {
        }

        public void stop() {
            this.stop = false;
            while (firstGatheredSize == 0) {
                this.stop = false;
            }
            this.stop = true;
        }

        public void run() {

            try {
                SingleChronicleHashReplication replicationHubForChannelIdMap2 =
                        SingleChronicleHashReplication.builder()
                                .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5086, new InetSocketAddress("localhost", 5085)))
                                .createWithId((byte) 2);
                ChronicleMap<String, Integer> channelIdMap2 = ChronicleMapBuilder.of(String.class, Integer.class)
                        .entries(Short.MAX_VALUE)
                        .instance()
                        .replicated(replicationHubForChannelIdMap2)
                        .create();
                this.chronicleMap = channelIdMap2;
                Object firstEl = 0;
                while (!stop) {
                    int size = channelIdMap2.size();
                    //System.out.println("Size=[" + size + "] on timestamp [" + Long.toString(System.currentTimeMillis()) + "]");
                    if (size != 0 && this.firstGatheredSize == 0) {
                        this.firstGatheredSize = size;
                        firstEl = channelIdMap2.get("0");
                    }
                }
                this.chronicleMap.close();
                firstGatheredElement = firstEl;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public int getFirstGatheredSize() {
            return this.firstGatheredSize;
        }

        public Object getFirstGatheredElement() {
            return this.firstGatheredElement;
        }

    }

    private class MapReaderWithListener implements Runnable {

        private volatile ChronicleMap<java.lang.String, Integer> chronicleMap;
        private boolean stop = false;
        private int firstGatheredSize = 0;
        private CacheMapListener mapListener;

        public MapReaderWithListener() {
        }

        public void stop() {
            this.stop = true;
        }

        public void run() {

            try {
                this.mapListener = new CacheMapListener<>();

                SingleChronicleHashReplication replicationHubForChannelIdMap2 =
                        SingleChronicleHashReplication.builder()
                                .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(5086, new InetSocketAddress("localhost", 5085)))
                                .createWithId((byte) 2);
                ChronicleMap<String, Integer> channelIdMap2 = ChronicleMapBuilder.of(String.class, Integer.class).eventListener(
                        (MapEventListener<String, Integer>) this.mapListener).entries(Short.MAX_VALUE)
                        .instance()
                        .replicated(replicationHubForChannelIdMap2)
                        .create();
                this.chronicleMap = channelIdMap2;
                while (!stop) {
                    channelIdMap2.size();
                }
                ;
                this.chronicleMap.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public int getFirstGatheredSize() {
            return this.firstGatheredSize;
        }

        public Object getFirstGatheredElement() {
            return this.mapListener.firstPuttedValue;
        }

    }

    public class CacheMapListener<K, V> extends MapEventListener<K, V> {


        private Object firstPuttedValue = null;

        public CacheMapListener() {

        }

        @Override
        public void onPut(K key, V newValue, @Nullable V replacedValue, boolean replicationEvent,
                          boolean added, boolean hasValueChanged, byte identifier,
                          byte replacedIdentifier, long timeStamp, long replacedTimeStamp) {

            if (this.firstPuttedValue == null) {
                this.firstPuttedValue = newValue;
            }

        }

        @Override
        public void onRemove(K key, V value, boolean replicationEvent, byte identifier,
                             byte replacedIdentifier, long timestamp, long replacedTimeStamp) {

        }
    }


}

