package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class TestOversizedReplicationHub {

    @Test
    public void testReplicationHubHandlesOverSizeEntries() throws IOException, InterruptedException {

        ChronicleMap<CharSequence, CharSequence> favoriteColourServer1, favoriteColourServer2;
        ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1, favoriteComputerServer2;

        int OVERSIZE_FACTOR = 100;
        char[] largeEntry = new char[100 * OVERSIZE_FACTOR];

        Arrays.fill(largeEntry, 'X');
        String largeString = new String(largeEntry);

        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<CharSequence, CharSequence> builder =
                    ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                            .keySize(10)
                            .valueSize(100);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086, new InetSocketAddress("localhost", 8087))
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
                            .keySize(10)
                            .valueSize(100);

            byte identifier = (byte) 2;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8087).heartBeatInterval(1, TimeUnit.SECONDS);

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

}
