package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Rob Austin
 */
public class TestReplication {
    @Test
    public void testAllDataGetsReplicated() throws InterruptedException {
        TcpTransportAndNetworkConfig tcpConfigServer1 =
                TcpTransportAndNetworkConfig.of(8082);

        TcpTransportAndNetworkConfig tcpConfigServer2 =
                TcpTransportAndNetworkConfig.of(8083, TcpUtil.localPort(8082));

        final ChronicleMap<Integer, Integer> map2 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .replication((byte) 2, tcpConfigServer2)
                .create();


        final ChronicleMap<Integer, Integer> map1 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .replication((byte) 3, tcpConfigServer1)
                .create();

        for (int i = 0; i < 500000; i++) {
            map1.put(i, i);
        }

        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (map2.size() == map1.size())
                break;
        }

        Assert.assertEquals(map1.size(), map2.size());
    }

}
