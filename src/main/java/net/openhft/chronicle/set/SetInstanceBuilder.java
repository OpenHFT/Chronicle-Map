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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
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
