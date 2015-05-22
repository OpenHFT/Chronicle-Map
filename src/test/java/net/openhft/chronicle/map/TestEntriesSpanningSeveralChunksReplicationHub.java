package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.ChronicleMapBuilder.of;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class TestEntriesSpanningSeveralChunksReplicationHub {

    @Ignore("performacne test")
    @Test
    public void testReplicationHubHandlesSpanningSeveralChunksEntries()
            throws IOException, InterruptedException {

        ChronicleMap<CharSequence, CharSequence> favoriteColourServer1, favoriteColourServer2;
        ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1, favoriteComputerServer2;

        int chunksPerEntry = 65;
        char[] largeEntry = new char[100 * chunksPerEntry];

        Arrays.fill(largeEntry, 'X');
        String largeString = new String(largeEntry);

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(10)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8021, TcpUtil.localPort(8022))
                    .heartBeatInterval(1, TimeUnit.SECONDS);

            ReplicationHub hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            ReplicationChannel channel = hubOnServer1.createChannel(channel1);
            favoriteColourServer1 = builder.instance()
                    .replicatedViaChannel(channel).create();

            favoriteColourServer1.put("peter", largeString);

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer1 = builder.instance()
                    .replicatedViaChannel(hubOnServer1.createChannel(channel2)).create();

            favoriteComputerServer1.put("peter", "dell");
        }

        // server 2 with  identifier = 2
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(10)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8022).heartBeatInterval(1, TimeUnit.SECONDS);

            ReplicationHub hubOnServer2 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);

            // this demotes favoriteColour
            short channel1 = (short) 1;

            favoriteColourServer2 = builder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel1)).create();

            favoriteColourServer2.put("rob", "blue");

            // this demotes favoriteComputer
            short channel2 = (short) 2;

            favoriteComputerServer2 = builder.instance()
                    .replicatedViaChannel(hubOnServer2.createChannel(channel2)).create();

            favoriteComputerServer2.put("rob", "mac");
            favoriteComputerServer2.put("daniel", "mac");
        }

        // allow time for the recompilation to resolve
        for (int t = 0; t < 2500; t++) {
            if (favoriteComputerServer2.equals(favoriteComputerServer1) &&
                    favoriteColourServer2.equals(favoriteColourServer1))
                break;
            Thread.sleep(1);
        }

        assertEquals(favoriteComputerServer1, favoriteComputerServer2);
        assertEquals(3, favoriteComputerServer2.size());

        assertEquals(favoriteColourServer1, favoriteColourServer2);
        assertEquals(2, favoriteColourServer1.size());

        favoriteColourServer1.close();
        favoriteComputerServer2.close();
        favoriteColourServer2.close();
        favoriteColourServer1.close();
    }

    public static final int NUMBER_OF_CHANNELS = 1000;

    @Ignore
    @Test(timeout = 100000)
    public void test1000Channels()
            throws IOException, InterruptedException {

        final HashMap<Short, ChronicleMap> map1 = new HashMap<>();
        final HashMap<Short, ChronicleMap> map2 = new HashMap<>();

        ReplicationHub hubOnServer1;
        ReplicationHub hubOnServer2;

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, Short> builder =
                    of(CharSequence.class, Short.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8059);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .maxNumberOfChannels(NUMBER_OF_CHANNELS)
                    .createWithId(identifier);

            for (short channelId = 1; channelId < NUMBER_OF_CHANNELS; channelId++) {
                ReplicationChannel channel = hubOnServer1.createChannel(channelId);
                ChronicleMap map = builder.instance().replicatedViaChannel(channel).create();

                map.put("key", channelId);
                map1.put(channelId, map);
            }
        }

        {
            ChronicleMapBuilder<CharSequence, Short> builder =
                    of(CharSequence.class, Short.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8054, TcpUtil.localPort(8059));

            hubOnServer2 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .maxNumberOfChannels(NUMBER_OF_CHANNELS)
                    .createWithId(identifier);

            for (short channelId = 1; channelId < NUMBER_OF_CHANNELS; channelId++) {
                ReplicationChannel channel = hubOnServer2.createChannel(channelId);
                ChronicleMap map = builder.instance().replicatedViaChannel(channel).create();

                map.put("key", channelId);
                map2.put(channelId, map);
            }
        }

        // check that the maps are the same, if the are not the same the test will timeout
        // we have to give a few seconds for the replication to propagate
        for (short channelId = 1; channelId < NUMBER_OF_CHANNELS; channelId++) {
            // allow time for the recompilation to resolve

            final ChronicleMap v1 = map1.get(channelId);
            final ChronicleMap v2 = map2.get(channelId);

            for (; ; ) {
                if (v1 == null || v2 == null) {
                    Thread.sleep(10);
                    continue;
                }

                if (v1.isEmpty() || v2.isEmpty()) {
                    Thread.sleep(10);
                    continue;
                }

                if (!v1.equals(v2)) {
                    Thread.sleep(10);
                    continue;
                }

                Assert.assertEquals(channelId, v1.get("key"));
                Assert.assertEquals(channelId, v2.get("key"));

                v1.put("key2", (short) 1);
                v2.put("key2", (short) 1);
                break;
            }

        }

        // check that the maps are the same, if the are not the same the test will timeout
        // we have to give a few seconds for the replication to propagate
        for (short channelId = 1; channelId < NUMBER_OF_CHANNELS; channelId++) {
            // allow time for the recompilation to resolve

            final ChronicleMap v1 = map1.get(channelId);
            final ChronicleMap v2 = map2.get(channelId);

            for (; ; ) {
                if (v1 == null || v2 == null) {
                    Thread.sleep(10);
                    continue;
                }

                if (v1.isEmpty() || v2.isEmpty()) {
                    Thread.sleep(10);
                    continue;
                }

                if (!v1.equals(v2)) {
                    Thread.sleep(10);
                    continue;
                }

                Assert.assertEquals((short)1, v1.get("key2"));
                Assert.assertEquals((short)1, v2.get("key2"));

                break;
            }

        }

        // close

        for (short channelId = 1; channelId < NUMBER_OF_CHANNELS; channelId++) {
            final ChronicleMap v1 = map1.get(channelId);
            if (v1 != null)
                v1.close();

            final ChronicleMap v2 = map2.get(channelId);
            if (v2 != null)
                v2.close();
        }
    }

    @Test
    public void testChannelWithDifferentTypes() throws IOException, InterruptedException {

        // server 1  - in this example only server 1 write any data
        ChronicleMap<CharSequence, CharSequence> charMap1;
        ChronicleMap<Integer, Integer> intMap1;
        {
            ChronicleMapBuilder<Integer, Integer> intMapBuilder =
                    of(Integer.class, Integer.class)
                            .entries(1000);

            ChronicleMapBuilder<CharSequence, CharSequence> charMapBuilder =
                    of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig.of(8025);

            ReplicationHub hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .maxNumberOfChannels(NUMBER_OF_CHANNELS)
                    .createWithId(identifier);

            ReplicationChannel channel1 = hubOnServer1.createChannel(1);

            intMap1 = intMapBuilder.instance()
                    .replicatedViaChannel(channel1)
                    .create();

            ReplicationChannel channel2 = hubOnServer1.createChannel(2);

            charMap1 = charMapBuilder.instance()
                    .replicatedViaChannel
                            (channel2)
                    .create();

            intMap1.put(1, 1);
            charMap1.put("Hello", "World");
        }

        // server 2
        ChronicleMap<CharSequence, CharSequence> charMap2;
        ChronicleMap<Integer, Integer> intMap2;

        {
            ChronicleMapBuilder<Integer, Integer> intMapBuilder =
                    of(Integer.class, Integer.class)
                            .entries(1000);

            ChronicleMapBuilder<CharSequence, CharSequence> charMapBuilder =
                    of(CharSequence.class, CharSequence.class)
                            .averageKeySize(10)
                            .averageValueSize(100)
                            .entries(1000);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig.of(8026, TcpUtil.localPort(8025));  // change the localhost to your server

            ReplicationHub hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .maxNumberOfChannels(NUMBER_OF_CHANNELS)
                    .createWithId(identifier);

            ReplicationChannel channel1 = hubOnServer1.createChannel(1);

            intMap2 = intMapBuilder.instance()
                    .replicatedViaChannel(channel1)
                    .create();

            ReplicationChannel channel2 = hubOnServer1.createChannel(2);

            charMap2 = charMapBuilder.instance().replicatedViaChannel
                    (channel2)
                    .create();
        }

        // allow time for the recompilation to resolve
        for (int t = 0; t < 3500; t++) {
            if (intMap1.equals(intMap2) &&
                    charMap1.equals(charMap2))
                break;
            Thread.sleep(1);
        }

        Assert.assertEquals(intMap1,intMap2);
        Assert.assertEquals(charMap1,charMap2);
    }
}

