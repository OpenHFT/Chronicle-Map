package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * Created by Rob Austin
 */
public class TestReplication {


    @Test
    public void testAllDataGetsReplicated() throws InterruptedException {


        TcpTransportAndNetworkConfig tcpConfigServer1 =
                TcpTransportAndNetworkConfig.of(8092);

        TcpTransportAndNetworkConfig tcpConfigServer2 =
                TcpTransportAndNetworkConfig.of(8093, new InetSocketAddress("localhost",
                        8092));

        final ChronicleMap<Integer, Integer> map2 = ChronicleMapBuilder.of(Integer.class,
                Integer.class)
                .replication((byte) 2, tcpConfigServer2)
                .create();


        final ChronicleMap<Integer, Integer> map1 = ChronicleMapBuilder.of(Integer.class,
                Integer.class)
                .replication((byte) 3, tcpConfigServer1)
                .create();


        for (int i = 0; i < 70000; i++) {
            map1.put(i, i);
        }

        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            System.out.println(map2.size());
        }


        Assert.assertEquals(map1.size(),map2.size());

    }

    /**
     * tests that replication works when the chronicle map that is getting the update is sharing
     * memory with the maps that is doing the update
     */
    @Test
    public void testReplicationUsingAProxyMapThatsUpdatedViaSharedMemory() throws Exception {

        ChronicleMap<String, String> server1 = null;
        ChronicleMap<String, String> server2 = null;

        // server 1  - for this test server1 and server 2 are on the same localhost but different
        // ports, to make it easier to run in a unit test
        {
            final File tempFile = File.createTempFile("test", "chron");


            server1 = ChronicleMapBuilder.of(String.class, String.class)
                    .replication(
                            (byte) 1).createPersistedTo
                            (tempFile);

            TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(8088);

            // user for replication only
            ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 1, serverConfig).createPersistedTo(tempFile);

        }

        // server 2- for this test server1 and server 2 are on the same localhost but different
        // ports, to make it easier to run in a unit test
        {
            TcpTransportAndNetworkConfig server2Config = TcpTransportAndNetworkConfig.of(8090, new
                    InetSocketAddress("localhost", 8088));

            server2 = ChronicleMapBuilder.of(String.class, String
                    .class).replication((byte) 2, server2Config).create();

        }

        final String expected = "value";
        server1.put("key", expected);

        String actual;


        // we have a while loop here as we have to wait a few seconds for the data to replicate
        do {
            actual = server2.get("key");
        }
        while (actual == null);

        Assert.assertEquals(expected, actual);

    }


    

}
