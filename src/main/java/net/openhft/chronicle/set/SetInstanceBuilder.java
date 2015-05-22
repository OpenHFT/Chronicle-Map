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
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.serialization.internal.DummyValue;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

final class SetInstanceBuilder<E> implements ChronicleHashInstanceBuilder<ChronicleSet<E>> {

    private final ChronicleHashInstanceBuilder<ChronicleMap<E, DummyValue>> mapBuilder;

    SetInstanceBuilder(ChronicleHashInstanceBuilder<ChronicleMap<E, DummyValue>> mapBuilder) {
        this.mapBuilder = mapBuilder;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        mapBuilder.replicated(identifier, tcpTransportAndNetwork);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicated(
            SingleChronicleHashReplication replication) {
        mapBuilder.replicated(replication);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> replicatedViaChannel(
            ReplicationChannel channel) {
        mapBuilder.replicatedViaChannel(channel);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> persistedTo(File file) {
        mapBuilder.persistedTo(file);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> name(String name) {
        mapBuilder.name(name);
        return this;
    }

    @Override
    public synchronized ChronicleSet<E> create() throws IOException {
        try {
            return new SetFromMap<>(mapBuilder.create());
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("A ChronicleMap")) {
                throw new IllegalStateException(
                        "A ChronicleSet has already been created using this instance config. " +
                                "Create a new instance config (builder.instance()) to create " +
                                "a new ChronicleSet instance");

            } else {
                throw e;
            }
        }
    }
}
