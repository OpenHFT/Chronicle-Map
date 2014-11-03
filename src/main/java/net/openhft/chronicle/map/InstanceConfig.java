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
import net.openhft.chronicle.hash.replication.SimpleReplication;
import net.openhft.chronicle.hash.replication.TcpConfig;

import java.io.File;
import java.io.IOException;

final class InstanceConfig<K, V>
        implements ChronicleHashInstanceConfig<ChronicleMap<K, V>> {

    final AbstractChronicleMapBuilder<K, V, ?> mapBuilder;
    final SimpleReplication simpleReplication;
    final ReplicationChannel channel;
    final File file;

    InstanceConfig(AbstractChronicleMapBuilder<K, V, ?> mapBuilder,
                   SimpleReplication simpleReplication,
                   ReplicationChannel channel,
                   File file) {
        this.mapBuilder = mapBuilder;
        this.simpleReplication = simpleReplication;
        this.channel = channel;
        this.file = file;
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicated(
            byte identifier, TcpConfig tcpTransportAndNetwork) {
        return replicated(SimpleReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).create(identifier));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicated(
            SimpleReplication replication) {
        return new InstanceConfig<>(mapBuilder, replication, null, file);
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> replicatedViaChannel(
            ReplicationChannel channel) {
        return new InstanceConfig<>(mapBuilder, null, channel, file);
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> persistedTo(File file) {
        return new InstanceConfig<>(mapBuilder, simpleReplication, channel, file);
    }

    @Override
    public ChronicleMap<K, V> create() throws IOException {
        return mapBuilder.create(this);
    }
}
