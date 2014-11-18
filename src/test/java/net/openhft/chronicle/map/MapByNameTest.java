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

import junit.framework.Assert;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class MapByNameTest {


    private ReplicationHubFindByName findMapByName;

    @Before
    public void setUp() throws IOException {
        final ReplicationHub replicationHub = ReplicationHub.builder()
                .maxEntrySize(10 * 1024)
                .tcpTransportAndNetwork(TcpTransportAndNetworkConfig.of(8243))
                .createWithId((byte) 1);

        findMapByName = new ReplicationHubFindByName(replicationHub);
    }

    @Test
    public void testSerializingBuilder() {
        {
            final ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                    .minSegments(2)
                    .name("map1")
                    .replication((byte) 1, TcpTransportAndNetworkConfig.of(8244))
                    .removeReturnsNull(true);

            findMapByName.add(builder);
        }

        final ChronicleMap<CharSequence, CharSequence> map = findMapByName.get("map1").create();
        map.put("hello", "world");

        Assert.assertEquals(map.get("hello"), "world");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializingBuilderUnknownMap() {
        findMapByName.get("hello");
    }


    // currently work in progress
    @Test
    @Ignore
    public void testConnectByName() throws IOException, InterruptedException {

        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder.of(CharSequence
                .class, CharSequence.class)
                .minSegments(2)
                .name("myMap")
                .removeReturnsNull(true);

        ReplicationHubFindByName mapByName = nodeDiscovery.mapByName();
        mapByName.add(builder);

        ChronicleMap<Object, Object> myMap2 = mapByName.create("myMap");

    }

    public static void main(String... args) throws IOException, InterruptedException {
        NodeDiscovery nodeDiscovery = new NodeDiscovery();

        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder.of(CharSequence
                .class, CharSequence.class)
                .minSegments(2)
                .name("myMap")
                .removeReturnsNull(true);


        ReplicationHubFindByName mapByName = nodeDiscovery.mapByName();
        mapByName.add(builder);


        // wait for ever
        for (; ; ) {
        }


    }

}
