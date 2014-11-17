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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
class FindMapByName {

    int maxEntrySize = 10*1024;


    private final ReplicationHub replicationHub;
    private Map<String,ChronicleMapBuilder> map;

    public FindMapByName(byte identifier) throws IOException {


        final TcpTransportAndNetworkConfig tcpConfig =
                TcpTransportAndNetworkConfig.of(8076)
                        .heartBeatInterval(1, TimeUnit.SECONDS);

        replicationHub = ReplicationHub.builder()
                .maxEntrySize(maxEntrySize)
                .tcpTransportAndNetwork(tcpConfig)
                .createWithId(identifier);

        map = (Map)ChronicleMapBuilder.of(CharSequence.class, ChronicleMapBuilder.class)
                .entrySize(300)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel((short) 1)).create();
    }

    public ChronicleMapBuilder get(String name) throws IOException {
        return map.get(name);
    }


    public void add(ChronicleMapBuilder builder) throws IOException {
        builder.name();
        this.map.put(builder.name(), builder);
    }
}
