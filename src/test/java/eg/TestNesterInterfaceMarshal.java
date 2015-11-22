/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eg;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.set.Builder;
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

        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig =
                TcpTransportAndNetworkConfig.of(8076).heartBeatInterval(1L, TimeUnit.SECONDS);

        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig1 =
                TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("127.0.0.1", 8076))
                        .heartBeatInterval(1L, TimeUnit.SECONDS);

        try (ChronicleMap<CharSequence, TestInstrumentVOInterface> map =
                     ChronicleMapBuilder
                             .of(CharSequence.class, TestInstrumentVOInterface.class)
                             .entries(5000L)
                             .averageKeySize(10)
                             .replication((byte) 1, tcpTransportAndNetworkConfig).create();

                ChronicleMap<CharSequence, TestInstrumentVOInterface> replicatedMap =
                        ChronicleMapBuilder
                                .of(CharSequence.class, TestInstrumentVOInterface.class)
                                .putReturnsNull(true)
                                .removeReturnsNull(true)
                                .entries(5000L).averageKeySize(10)
                                .replication((byte) 2, tcpTransportAndNetworkConfig1).create()) {
            //Store some data into MAP1
            TestInstrumentVOInterface intrumentVOInterface = map.newValueInstance();


            try (Closeable c = map.acquireContext("KEY1", intrumentVOInterface)) {
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

            Builder.waitTillEqual(map, replicatedMap, 5000);

            Assert.assertEquals(map, replicatedMap);
            Assert.assertTrue(!replicatedMap.isEmpty());

            System.out.println(replicatedMap.get("KEY1"));
        }
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


