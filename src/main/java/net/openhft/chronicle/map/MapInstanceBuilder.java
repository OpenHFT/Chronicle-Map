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

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

final class MapInstanceBuilder<K, V>
        implements ChronicleHashInstanceBuilder<ChronicleMap<K, V>>, Serializable {

    final ChronicleMapBuilder<K, V> mapBuilder;
    transient SingleChronicleHashReplication singleHashReplication;
    transient ReplicationChannel channel;
    transient File file;
    String name;

    volatile boolean used = false;

    MapInstanceBuilder(ChronicleMapBuilder<K, V> mapBuilder,
                       SingleChronicleHashReplication singleHashReplication) {
        this.mapBuilder = mapBuilder;
        this.singleHashReplication = singleHashReplication;
    }

    @Override
    public MapInstanceBuilder<K, V> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return replicated(SingleChronicleHashReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier));
    }

    @Override
    public MapInstanceBuilder<K, V> replicated(SingleChronicleHashReplication replication) {
        singleHashReplication = replication;
        channel = null;
        return this;
    }

    @Override
    public MapInstanceBuilder<K, V> replicatedViaChannel(ReplicationChannel channel) {
        singleHashReplication = null;
        this.channel = channel;
        return this;
    }

    @Override
    public MapInstanceBuilder<K, V> persistedTo(File file) {
        this.file = file;
        return this;
    }

    @Override
    public MapInstanceBuilder<K, V> name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public synchronized ChronicleMap<K, V> create() throws IOException {
        if (!used) {
            ChronicleMap<K, V> map = mapBuilder.create(this, false, false);
            // Set used = true, if map created successfully.
            // If exception is thrown in mapBuilder.create(), the next line is not executed.
            used = true;
            return map;
        } else {
            throw alreadyCreated();
        }
    }

    @Override
    public synchronized ChronicleMap<K, V> recover(boolean sameBuilderConfig) throws IOException {
        if (file == null)
            throw new IllegalStateException("persistedTo(file) must be configured for " +
                    "ChronicleHashInstanceBuilder, for recover()");
        if (!used) {
            ChronicleMap<K, V> map = mapBuilder.create(this, true, sameBuilderConfig);
            used = true;
            return map;
        } else {
            throw alreadyCreated();
        }
    }

    private static IllegalStateException alreadyCreated() {
        return new IllegalStateException(
                "A ChronicleHash has already been created using this " +
                        "ChronicleHashInstanceBuilder. Create a new instance builder " +
                        "(chronicleHashBuilder.instance()) to create another Chronicle Hash");
    }
}
