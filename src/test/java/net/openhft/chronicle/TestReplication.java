package net.openhft.chronicle;

import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Rob Austin
 */
public class TestReplication {


    @Test
    public void testAllDataGetsReplicated() throws InterruptedException {


        TcpTransportAndNetworkConfig tcpConfigServer1 =
                TcpTransportAndNetworkConfig.of(8082);

        TcpTransportAndNetworkConfig tcpConfigServer2 =
                TcpTransportAndNetworkConfig.of(8083, new InetSocketAddress("localhost",
                        8082));

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


        Assert.assertEquals(map1.size(), map2.size());

    }


    public static final int SIZE = 1000;
    private Map<Short, ChronicleMap<Short, Short>> maps = new HashMap<>();


    private ReplicationHub hubOnServer1;

    @Test
    public void testAllDataGetsReplicated2() throws InterruptedException, IOException, ExecutionException {

        try
        // server 1 with  identifier = 1
        {
            ChronicleMapBuilder<Short, Short> builder =
                    ChronicleMapBuilder.of(Short.class, Short.class)
                            .entries(1000);

            byte identifier = (byte) 1;

            TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086);

            hubOnServer1 = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .maxNumberOfChannels(SIZE)
                    .createWithId(identifier);

            for (short channel1 = 3; channel1 < SIZE; channel1++) {
                ChronicleMap<Short, Short> map = builder.instance()
                        .replicatedViaChannel(hubOnServer1.createChannel(channel1)).create();

                for (int i = 0; i < 10; i++) {
                    map.put((short) i, (short) i);
                }

                maps.put(channel1, map);

                System.out.println("" + channel1);


            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        for (; ; ) {
            Thread.sleep(1000);
        }

    }


}

