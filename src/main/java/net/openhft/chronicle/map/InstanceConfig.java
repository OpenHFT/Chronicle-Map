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

final class InstanceConfig<K, V>
        implements ChronicleHashInstanceConfig<ChronicleMap<K, V>> {

    final AbstractChronicleMapBuilder<K, V, ?> mapBuilder;
    final SingleChronicleHashReplication singleHashReplication;
    final ReplicationChannel channel;
    final File file;

    private boolean used = false;

    InstanceConfig(AbstractChronicleMapBuilder<K, V, ?> mapBuilder,
                   SingleChronicleHashReplication singleHashReplication,
                   ReplicationChannel channel,
                   File file) {
        this.mapBuilder = mapBuilder;
        this.singleHashReplication = singleHashReplication;
        this.channel = channel;
        this.file = file;
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return replicated(SingleChronicleHashReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicated(
            SingleChronicleHashReplication replication) {
        return new InstanceConfig<>(mapBuilder, replication, null, file);
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicatedViaChannel(
            ReplicationChannel channel) {
        return new InstanceConfig<>(mapBuilder, null, channel, file);
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> persistedTo(File file) {
        return new InstanceConfig<>(mapBuilder, singleHashReplication, channel, file);
    }

    @Override
    public synchronized ChronicleMap<K, V> create() throws IOException {
        if (used)
            throw new IllegalStateException(
                    "A ChronicleMap has already been created using this instance config. " +
                            "Create a new instance config (builder.instance() or call any " +
                            "configuration method on this instance config) to create a new " +
                            "ChronicleMap instance");
        used = true;
        return mapBuilder.create(this);
    }

}
