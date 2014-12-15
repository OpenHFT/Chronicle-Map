package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by Vanitha on 12/5/2014.
 */
public class TestNesterInterfaceMarshal {


    /**
     * Test that both maps are created first and then populated the first map so that second will
     * get the contents
     *
     * @throws Exception
     */
    @Test
    public void testReplicatedMap() throws Exception {
        ChronicleMap<CharSequence, TestIntrumentVOInterface> map = createReplicatedMap1();
        ChronicleMap<CharSequence, TestIntrumentVOInterface> replicatedMap = createReplicatedMap2();


        //Store some data into MAP1
        TestIntrumentVOInterface intrumentVOInterface = map.newValueInstance();
        intrumentVOInterface.setSymbol("Flyer");
        intrumentVOInterface.setCurrencyCode("USA");
        intrumentVOInterface.setSizeOfInstrumentIDArray(2);

        TestIntrumentVOInterface.TestInstrumentIDVOInterface instrumentIDVOInterface = intrumentVOInterface.getInstrumentIDAt(0);
        instrumentIDVOInterface.setIdSource("CUSIP");
        instrumentIDVOInterface.setSecurityId("TEST");
        intrumentVOInterface.setInstrumentIDAt(0, instrumentIDVOInterface);
        map.put("KEY1", intrumentVOInterface);


        Assert.assertNotNull(map);
        Assert.assertNotNull(replicatedMap);


        int t = 0;
        for (; t < 5000; t++) {
            if (map.equals(replicatedMap))
                break;
            Thread.sleep(1);
        }

        Assert.assertEquals(map, replicatedMap);
        Assert.assertTrue(!replicatedMap.isEmpty());

        System.out.println(map.get("KEY1"));
        System.out.println(replicatedMap.get("KEY1"));

        map.close();
        replicatedMap.close();

    }

    protected ChronicleMap createReplicatedMap1() {

        ChronicleMap chronicleMap = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig = TcpTransportAndNetworkConfig.of(8076).heartBeatInterval(1L, TimeUnit.SECONDS);
        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder.of(CharSequence.class, TestIntrumentVOInterface.class)
                    .entries(5000L).entrySize(2000)
                    .replication((byte) 1, tcpTransportAndNetworkConfig);


            chronicleMap = builder.create();

        } catch (Exception e) {
            System.out.println("Error(s) creating instrument cache: " + e);
        }
        return chronicleMap;
    }

    protected ChronicleMap createReplicatedMap2() {

        ChronicleMap chronicleMap = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig = TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076)).heartBeatInterval(1L, TimeUnit.SECONDS);

        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder.of(CharSequence.class, TestIntrumentVOInterface.class)
                    .putReturnsNull(true)
                    .removeReturnsNull(true)
                    .entries(5000L).entrySize(2000)
                    .replication((byte) 2, tcpTransportAndNetworkConfig);


            chronicleMap = builder.create();

        } catch (Exception e) {
            System.out.println("*********************Error(s) creating launcher instrument cache: " + e);
        }
        return chronicleMap;
    }

}


