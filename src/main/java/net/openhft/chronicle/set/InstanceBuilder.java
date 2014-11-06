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

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SimpleReplication;
import net.openhft.chronicle.hash.replication.TcpConfig;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

final class InstanceBuilder<E> implements ChronicleHashInstanceBuilder<ChronicleSet<E>> {

    private final ChronicleHashInstanceBuilder<ChronicleMap<E, DummyValue>> mapBuilder;

    InstanceBuilder(ChronicleHashInstanceBuilder<ChronicleMap<E, DummyValue>> mapBuilder) {
        this.mapBuilder = mapBuilder;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicated(
            byte identifier, TcpConfig tcpTransportAndNetwork) {
        return new InstanceBuilder<>(mapBuilder.replicated(identifier, tcpTransportAndNetwork));
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicated(SimpleReplication replication) {
        return new InstanceBuilder<>(mapBuilder.replicated(replication));
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicatedViaChannel(
            ReplicationChannel channel) {
        return new InstanceBuilder<>(mapBuilder.replicatedViaChannel(channel));
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> persistedTo(File file) {
        return new InstanceBuilder<>(mapBuilder.persistedTo(file));
    }

    @Override
    public ChronicleSet<E> create() throws IOException {
        return new SetFromMap<>(mapBuilder.create());
    }
}
