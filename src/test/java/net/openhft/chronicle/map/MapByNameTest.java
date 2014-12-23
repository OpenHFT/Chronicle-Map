/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class MapByNameTest {

    private ReplicationHubFindByName<CharSequence> findMapByName;

    @Before
    public void setUp() throws IOException {
        final ReplicationHub replicationHub = ReplicationHub.builder()
                .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(8243))
                .createWithId((byte) 1);

        findMapByName = new ReplicationHubFindByName(replicationHub);
    }

    @Test
    public void testSerializingBuilder() throws TimeoutException, InterruptedException, IOException {

        final MapInstanceConfig<CharSequence, CharSequence> config =
                (MapInstanceConfig<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .keySize("hello".length()).valueSize("world".length())
                                .entries(2)
                                .minSegments(2)
                                .replication((byte) 1, TcpTransportAndNetworkConfig.of(8244))
                                .removeReturnsNull(true)
                                .instance().name("map1");

        final ChronicleMap<CharSequence, CharSequence> map = findMapByName.create(config);
        map.put("hello", "world");

        assertEquals(map.get("hello"), "world");
    }

    //  @Test(expected = IllegalArgumentException.class)
    //  public void testSerializingBuilderUnknownMap() throws TimeoutException, InterruptedException {
    //      findMapByName.get("hello");
    //  }

    @Test
    @Ignore("currently work in progress")
    public void testConnectByName() throws IOException, InterruptedException, TimeoutException {

        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        MapInstanceConfig<CharSequence, CharSequence> config =
                (MapInstanceConfig<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .minSegments(2)
                                .removeReturnsNull(true)
                                .instance()
                                .name("myMap");

        ReplicationHubFindByName<CharSequence> mapByName = nodeDiscovery.mapByName();
        //  mapByName.add(config);

        ChronicleMap<CharSequence, CharSequence> myMap2 = mapByName.from("myMap");
    }

    public static void main(String... args) throws IOException, InterruptedException, TimeoutException {
        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        final ReplicationHubFindByName<CharSequence> mapByName = nodeDiscovery.mapByName();
        Thread.sleep(2000);

        MapInstanceConfig<CharSequence, CharSequence> config =
                (MapInstanceConfig<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .minSegments(2)
                                .entries(2)
                                .removeReturnsNull(true)
                                .instance()
                                .name("myMap6");

        ChronicleMap<String, String> map = mapByName.create(config);
        map.put("hello", "world");

        //    final ChronicleMap<CharSequence, CharSequence> map = mapByName.from("myMap5");

        // allow time for replication
        Thread.sleep(2000);
        System.out.print(map);

        map.close();
    }

}
