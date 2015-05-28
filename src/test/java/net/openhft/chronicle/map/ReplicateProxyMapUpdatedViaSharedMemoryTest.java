package net.openhft.chronicle.map;

import org.junit.Assert;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by khoxsey on 5/22/15.
 */
public class ReplicateProxyMapUpdatedViaSharedMemoryTest {

    /**
     * test 2 maps on one server ( one that replicates vai TCP/IP and one that just shares memory )
     * can connect to a remote map
     * connect to a remote map
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testReplicateProxyMapUpdatedViaSharedMemory() throws IOException, InterruptedException {

        ChronicleMap<String, String> server1 = null;
        ChronicleMap<String, String> server2 = null;
        ChronicleMap<String, String> replicationMap = null;

        {
            final File tempFile = File.createTempFile("temp", "chron");

            server1 = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 1).createPersistedTo(tempFile);

            TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(7077);
            replicationMap = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 1, serverConfig).createPersistedTo(tempFile);
        }

        {
            TcpTransportAndNetworkConfig server2Config =
                    TcpTransportAndNetworkConfig.of(6161, new InetSocketAddress("localhost", 7077));

            server2 = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 2, server2Config).create();
        }

        final String expected = "value";
        Thread.sleep(1000);
        server1.put("key", expected);

        String actual;
        do {
            actual = server2.get("key");
        } while (actual == null);

        Assert.assertEquals(expected, actual);

        server1.close();
        server2.close();
        replicationMap.close();
    }
}
