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

import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

final class MapInstanceConfig<K, V>
        implements ChronicleHashInstanceConfig<ChronicleMap<K, V>>, Serializable{

    final ChronicleMapBuilder<K, V> mapBuilder;
    final SingleChronicleHashReplication singleHashReplication;
    final ReplicationChannel channel;
    final File file;
    final String name;

    final AtomicBoolean used;

    MapInstanceConfig(ChronicleMapBuilder<K, V> mapBuilder,
                      SingleChronicleHashReplication singleHashReplication,
                      ReplicationChannel channel,
                      File file, String name, AtomicBoolean used) {
        this.mapBuilder = mapBuilder;
        this.singleHashReplication = singleHashReplication;
        this.channel = channel;
        this.file = file;
        this.name = name;
        this.used = used;
    }

    @Override
    public MapInstanceConfig<K, V> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return replicated(SingleChronicleHashReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier));
    }

    @Override
    public MapInstanceConfig<K, V> replicated(SingleChronicleHashReplication replication) {
        return new MapInstanceConfig<>(mapBuilder, replication, null, file, name, used);
    }

    @Override
    public MapInstanceConfig<K, V> replicatedViaChannel(ReplicationChannel channel) {
        return new MapInstanceConfig<>(mapBuilder, null, channel, file, name, used);
    }

    @Override
    public MapInstanceConfig<K, V> persistedTo(File file) {
        return new MapInstanceConfig<>(mapBuilder, singleHashReplication, channel, file, name, used);
    }

    @Override
    public MapInstanceConfig<K, V> name(String name) {
        return new MapInstanceConfig<>(mapBuilder, singleHashReplication, channel, file, name, used);
    }

    @Override
    public synchronized ChronicleMap<K, V> create() throws IOException {
        if (!used.getAndSet(true)) {
            return mapBuilder.create(this);
        } else {
            throw new IllegalStateException(
                    "A ChronicleMap has already been created using this instance config chain. " +
                            "Create a new instance config (builder.instance()) to create a new " +
                            "ChronicleMap instance");
        }
    }

}
