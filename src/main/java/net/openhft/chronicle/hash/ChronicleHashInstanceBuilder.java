/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;

import java.io.File;
import java.io.IOException;

public interface ChronicleHashInstanceBuilder<C extends ChronicleHash> {

    ChronicleHashInstanceBuilder<C> replicated(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork);

    ChronicleHashInstanceBuilder<C> replicated(SingleChronicleHashReplication replication);

    /**
     * Configures replication of the hash containers, created by this builder, via so called
     * "channels". See
     * <a href="https://github.com/OpenHFT/Chronicle-Map#channels-and-channelprovider">the
     * section about Channels and ChannelProvider in ChronicleMap manual</a> for more information.
     * <p>
     * <p>Another way to establish replication is {@link #replicated(SingleChronicleHashReplication)} method
     * or it's shortcut: {@link #replicated(byte, TcpTransportAndNetworkConfig)}.
     * <p>
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     * <p>
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by means of this method, {@link #replicated(SingleChronicleHashReplication)}
     * or {@link #replicated(byte, TcpTransportAndNetworkConfig)} method calls.
     *
     * @param channel the channel responsible for gathering updates of hash containers,
     *                created by this builder, and replicating them over network
     * @return this builder object back
     * @see #replicated(SingleChronicleHashReplication)
     */
    ChronicleHashInstanceBuilder<C> replicatedViaChannel(ReplicationChannel channel);

    ChronicleHashInstanceBuilder<C> persistedTo(File file);

    ChronicleHashInstanceBuilder<C> name(String name);

    C create() throws IOException;
}
