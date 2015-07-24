package net.openhft.chronicle.map;

import eg.TestInstrumentVOInterface;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author Rob Austin.
 */
public class TcpReplicationWithAcquireContextTest {


    @Test
    public void testReplicationWithAcquireUsingLocked() throws Exception {


        TcpTransportAndNetworkConfig config1 = TcpTransportAndNetworkConfig.of(8076);

        ChronicleMap<CharSequence, TestInstrumentVOInterface> map2a = null;
        ChronicleMap<CharSequence, TestInstrumentVOInterface> map1a = null;

        try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map1 = ChronicleMapBuilder.of
                (CharSequence.class, TestInstrumentVOInterface.class)
                .replication((byte) 1, config1)
                .create()) {

            map1a = map1;

            TcpTransportAndNetworkConfig config2 =
                    TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076));


            try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map2 =
                         ChronicleMapBuilder.of(CharSequence.class,
                                 TestInstrumentVOInterface.class)
                                 .replication((byte) 2, config2)
                                 .create()) {

                map2a = map2;

                //Store some data into MAP1
                TestInstrumentVOInterface instrumentVOInterface = map1.newValueInstance();


                try (net.openhft.chronicle.core.io.Closeable c =
                             map1.acquireContext("KEY1", instrumentVOInterface)) {
                    instrumentVOInterface.setSymbol("Flyer");
                    instrumentVOInterface.setCurrencyCode("USA");
                }


                int t = 0;
                for (; t < 5000; t++) {
                    if (map1.equals(map2))
                        break;
                    Thread.sleep(1);
                }


                Assert.assertEquals(map1a, map2a);
            }
        }


    }


    @Test
    public void testReplicationWithEmptyOffHeapObject() throws Exception {


        TcpTransportAndNetworkConfig config1 = TcpTransportAndNetworkConfig.of(8076);

        ChronicleMap<CharSequence, TestInstrumentVOInterface> map2a = null;
        ChronicleMap<CharSequence, TestInstrumentVOInterface> map1a = null;

        try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map1 = ChronicleMapBuilder.of
                (CharSequence.class,
                        TestInstrumentVOInterface
                                .class)
                .entries(5000L).averageKeySize("hello".length())
                .replication((byte) 1, config1).create()) {

            map1a = map1;

            TcpTransportAndNetworkConfig config2 = TcpTransportAndNetworkConfig
                    .of(8077, new InetSocketAddress("127.0.0.1", 8076));


            try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map2 =
                         ChronicleMapBuilder.of(CharSequence.class,
                                 TestInstrumentVOInterface.class)
                                 .putReturnsNull(true)
                                 .removeReturnsNull(true)
                                 .replication((byte) 2, config2)
                                 .entries(5000L).averageKeySize("hello".length()).create()) {

                map2a = map2;

                //Store some data into MAP1
                TestInstrumentVOInterface instrumentVOInterface = map1.newValueInstance();

                map2a.put("hello", instrumentVOInterface);


                int t = 0;
                for (; t < 5000; t++) {
                    if (map1.equals(map2))
                        break;
                    Thread.sleep(1);
                }

                Assert.assertEquals(map1a, map2a);
            }
        }


    }


    @Test
    public void testReplicationWithOffHeapObject() throws Exception {


        TcpTransportAndNetworkConfig config1 = TcpTransportAndNetworkConfig.of(8076);

        ChronicleMap<CharSequence, TestInstrumentVOInterface> map2a = null;
        ChronicleMap<CharSequence, TestInstrumentVOInterface> map1a = null;

        try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map1 = ChronicleMapBuilder.of
                (CharSequence.class,
                        TestInstrumentVOInterface
                                .class)
                .entries(5000L).averageKeySize("hello".length())
                .replication((byte) 1, config1).create()) {

            map1a = map1;

            TcpTransportAndNetworkConfig config2 =
                    TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076));


            try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map2 =
                         ChronicleMapBuilder.of(CharSequence.class,
                                 TestInstrumentVOInterface.class)
                                 .putReturnsNull(true)
                                 .removeReturnsNull(true)
                                 .replication((byte) 2, config2)
                                 .entries(5000L).averageKeySize("hello".length()).create()) {

                map2a = map2;

                //Store some data into MAP1
                TestInstrumentVOInterface instrumentVOInterface = map1.newValueInstance();
                instrumentVOInterface.setSymbol("Flyer");

                map2a.put("hello", instrumentVOInterface);


                int t = 0;
                for (; t < 5000; t++) {
                    if (map1.equals(map2))
                        break;
                    Thread.sleep(1);
                }

                Assert.assertEquals(map1a, map2a);
            }
        }


    }

}
