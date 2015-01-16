package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapKeyContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by Vanitha on 12/5/2014.
 */
@Ignore("a test written by someone on the google groups")
public class TestNesterInterfaceMarshal {


    /**
     * Test that both maps are created first and then populated the first map so that second will
     * get the contents
     *
     * @throws Exception
     */
    @Test

    public void testReplicatedMap() throws Exception {

        ChronicleMap chronicleMap = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig =
                TcpTransportAndNetworkConfig.of(8076).heartBeatInterval(1L, TimeUnit.SECONDS);
        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder
                    .of(CharSequence.class, TestInstrumentVOInterface.class)
                    .entries(5000L)
                    .averageKeySize(10)
                    .replication((byte) 1, tcpTransportAndNetworkConfig);


            chronicleMap = builder.create();

        } catch (Exception e) {
            System.out.println("Error(s) creating instrument cache: " + e);
        }
        ChronicleMap<CharSequence, TestInstrumentVOInterface> map = chronicleMap;

        ChronicleMap chronicleMap1 = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig1 =
                TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076))
                        .heartBeatInterval(1L, TimeUnit.SECONDS);

        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder
                    .of(CharSequence.class, TestInstrumentVOInterface.class)
                    .putReturnsNull(true)
                    .removeReturnsNull(true)
                    .entries(5000L).averageKeySize(10)
                    .replication((byte) 2, tcpTransportAndNetworkConfig1);


            chronicleMap1 = builder.create();

        } catch (Exception e) {
            System.out.println("*********************Error(s) creating launcher " +
                    "instrument cache: " + e);
        }
        ChronicleMap<CharSequence, TestInstrumentVOInterface> replicatedMap = chronicleMap1;


        //Store some data into MAP1
        TestInstrumentVOInterface intrumentVOInterface = map.newValueInstance();


        try (MapKeyContext<TestInstrumentVOInterface> wc = map.acquireContext
                ("KEY1", intrumentVOInterface)) {
            intrumentVOInterface.setSymbol("Flyer");
            intrumentVOInterface.setCurrencyCode("USA");
            intrumentVOInterface.setSizeOfInstrumentIDArray(2);

            //   TestIntrumentVOInterface.TestInstrumentIDVOInterface instrumentIDVOInterface =
            intrumentVOInterface.getInstrumentIDAt(0);
            //   instrumentIDVOInterface.setIdSource("CUSIP");
            //   instrumentIDVOInterface.setSecurityId("TEST");

            // intrumentVOInterface.setInstrumentIDAt(0, instrumentIDVOInterface);


            //  Assert.assertNotNull(map);
            //  Assert.assertNotNull(replicatedMap);
        }


        int t = 0;
        for (; t < 5000; t++) {
            if (map.equals(replicatedMap))
                break;
            Thread.sleep(1);
        }

        Assert.assertEquals(map, replicatedMap);
        Assert.assertTrue(!replicatedMap.isEmpty());

        System.out.println(((TestInstrumentVOInterface.TestInstrumentIDVOInterface) map.get
                ("KEY1")).getIdSource());
        System.out.println(replicatedMap.get("KEY1"));

        map.close();
        replicatedMap.close();

    }

    protected ChronicleMap createReplicatedMap1() {

        ChronicleMap chronicleMap = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig =
                TcpTransportAndNetworkConfig.of(8076).heartBeatInterval(1L, TimeUnit.SECONDS);
        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder
                    .of(CharSequence.class, TestInstrumentVOInterface.class)
                    .entries(5000L)
                    .averageKeySize(10)
                    .replication((byte) 1, tcpTransportAndNetworkConfig);


            chronicleMap = builder.create();

        } catch (Exception e) {
            System.out.println("Error(s) creating instrument cache: " + e);
        }
        return chronicleMap;
    }

    protected ChronicleMap createReplicatedMap2() {

        ChronicleMap chronicleMap = null;
        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig =
                TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076))
                        .heartBeatInterval(1L, TimeUnit.SECONDS);

        try {
            ChronicleMapBuilder builder = ChronicleMapBuilder
                    .of(CharSequence.class, TestInstrumentVOInterface.class)
                    .putReturnsNull(true)
                    .removeReturnsNull(true)
                    .entries(5000L)
                    .averageKeySize(10)
                    .replication((byte) 2, tcpTransportAndNetworkConfig);


            chronicleMap = builder.create();

        } catch (Exception e) {
            System.out.println("*********************Error(s) creating launcher " +
                    "instrument cache: " + e);
        }
        return chronicleMap;
    }

}


