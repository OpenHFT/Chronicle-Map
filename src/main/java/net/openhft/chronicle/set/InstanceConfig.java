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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

final class InstanceConfig<E> implements ChronicleHashInstanceConfig<ChronicleSet<E>> {

    private final ChronicleHashInstanceConfig<ChronicleMap<E, DummyValue>> mapConfig;

    InstanceConfig(ChronicleHashInstanceConfig<ChronicleMap<E, DummyValue>> mapConfig) {
        this.mapConfig = mapConfig;
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleSet<E>> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return new InstanceConfig<>(mapConfig.replicated(identifier, tcpTransportAndNetwork));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleSet<E>> replicated(SingleChronicleHashReplication replication) {
        return new InstanceConfig<>(mapConfig.replicated(replication));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleSet<E>> replicatedViaChannel(
            ReplicationChannel channel) {
        return new InstanceConfig<>(mapConfig.replicatedViaChannel(channel));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleSet<E>> persistedTo(File file) {
        return new InstanceConfig<>(mapConfig.persistedTo(file));
    }

    @Override
    public ChronicleSet<E> create() throws IOException {
        return new SetFromMap<>(mapConfig.create());
    }
}
