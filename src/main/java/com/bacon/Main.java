package com.bacon;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        ChronicleMap<String, String> server1 = null;
        ChronicleMap<String, String> server2 = null;
        ChronicleMap<String, String> replicaitonMap;

        {
            final File tempFile = File.createTempFile("test", "chron");

            server1 = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 1).createPersistedTo(tempFile);

            TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(8088);

            replicaitonMap = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 1, serverConfig).createPersistedTo(tempFile);
        }

        {
            InetSocketAddress s2address = new InetSocketAddress("localhost", 8088);
            TcpTransportAndNetworkConfig server2Config = TcpTransportAndNetworkConfig.of(8090, s2address);

            server2 = ChronicleMapBuilder.of(String.class, String.class)
                    .replication((byte) 2, server2Config).create();
        }

        final String expected = "value";

        server1.put("key", expected);

        String actual;

        do {
            actual = server2.get("key");
        } while (actual == null);

        server1.close();
        server2.close();
        replicaitonMap.close();
    }
}
