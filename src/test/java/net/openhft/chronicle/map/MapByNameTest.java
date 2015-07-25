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
@Ignore
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

        final MapInstanceBuilder<CharSequence, CharSequence> config =
                (MapInstanceBuilder<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .averageKeySize("hello".length()).averageValueSize("world".length())
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

        MapInstanceBuilder<CharSequence, CharSequence> config =
                (MapInstanceBuilder<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .minSegments(2)
                                .removeReturnsNull(true)
                                .instance()
                                .name("myMap");

        ReplicationHubFindByName<CharSequence> mapByName = nodeDiscovery.mapByName();
        //  mapByName.add(config);

        ChronicleMap<CharSequence, CharSequence> myMap2 = mapByName.from("myMap");
    }

    @Ignore("causing issues on windows")
    @Test
    public void testReplicationHubSerialization() throws IOException, InterruptedException,
            TimeoutException {
        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        final ReplicationHubFindByName<CharSequence> mapByName = nodeDiscovery.mapByName();
        Thread.sleep(2000);

        MapInstanceBuilder<CharSequence, CharSequence> config =
                (MapInstanceBuilder<CharSequence, CharSequence>)
                        ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                .minSegments(2)
                                .entries(2)
                                .removeReturnsNull(true)
                                .instance()
                                .name("myMap6");

        mapByName.create(config);

    }

    public static void main(String... args) throws IOException, InterruptedException, TimeoutException {
        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        final ReplicationHubFindByName<CharSequence> mapByName = nodeDiscovery.mapByName();
        Thread.sleep(2000);

        MapInstanceBuilder<CharSequence, CharSequence> config =
                (MapInstanceBuilder<CharSequence, CharSequence>)
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
