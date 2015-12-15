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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.io.File;
import java.io.IOException;

/**
 * A disposable builder of {@link ChronicleHash} instances, allowing to set unique configurations
 * using "chaining" pattern.
 *
 * <p>Usage example: <pre>{@code
 * ChronicleMap<LongValue, Order> = ChronicleMap
 *     .of(LongValue.class, Order.class)
 *     .entries(10_000_000)
 *     .instance()      // a ChronicleHashInstanceBuilder is returned from this method
 *                      // continue "chaining"
 *     .replicated(...)
 *     .name(...)
 *     .persistedTo(...)
 *     .create();
 * }</pre>
 *
 * @param <H> the container type, created by this builder, {@link ChronicleMap} or
 * {@link ChronicleSet}
 * @see ChronicleHashBuilder#instance()
 */
public interface ChronicleHashInstanceBuilder<H extends ChronicleHash> {

    ChronicleHashInstanceBuilder<H> replicated(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork);

    ChronicleHashInstanceBuilder<H> replicated(SingleChronicleHashReplication replication);

    /**
     * Configures replication of the hash container, which is going to be created by this builder,
     * via so called "channels". See <a
     * href="https://github.com/OpenHFT/Chronicle-Map#channels-and-channelprovider">the section
     * about Channels and ChannelProvider in ChronicleMap manual</a> for more information.
     *
     * <p>Another way to establish replication is {@link #replicated(SingleChronicleHashReplication)
     * } method or it's shortcut: {@link #replicated(byte, TcpTransportAndNetworkConfig)}.
     *
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     *
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by means of this method, {@link #replicated(SingleChronicleHashReplication)}
     * or {@link #replicated(byte, TcpTransportAndNetworkConfig)} method calls.
     *
     * @param channel the channel responsible for gathering updates of hash containers,
     *                created by this builder, and replicating them over network
     * @return this builder object back
     * @see #replicated(SingleChronicleHashReplication)
     */
    ChronicleHashInstanceBuilder<H> replicatedViaChannel(ReplicationChannel channel);

    /**
     * Configures the name for the Chronicle Hash, which is going to be created by this builder. It
     * is used to name background threads, run by replicated Chronicle Hash instances.
     *
     * @param name the name for the Chronicle Hash is going to be created by this builder
     * @return this builder back
     */
    ChronicleHashInstanceBuilder<H> name(String name);

    /**
     * Configures the file, the Chronicle Hash, which is going to be created by this builder, should
     * be persisted to. See {@link ChronicleHashBuilder#createPersistedTo(File)} for more
     * information on Chronicle Hash persistence. If the given file is {@code null}, in-memory
     * Chronicle Hash is created, like by {@link ChronicleHashBuilder#create()}.
     *
     * <p>By default, persistence file is configured to {@code null}, i. e. the in-memory Chronicle
     * Hash instance is created.
     *
     * @param file the file the Chronicle Hash, which is going to be created by this builder, should
     *             be persisted to, to {@code null}, if the Chronicle Hash should be purely
     *             in-memory
     * @return this builder back
     */
    ChronicleHashInstanceBuilder<H> persistedTo(File file);

    /**
     * Creates (or opens existing, if {@link #persistedTo(File)} is configured with the existing
     * file) a Chronicle Hash from this builder. After this method is called once the builder
     * couldn't be used to create new Chronicle Hash instances, a new {@code
     * ChronicleHashInstanceBuilder} should be created via {@link ChronicleHashBuilder#instance()}.
     *
     * @return a new in-memory or persisted Chronicle Hash instance
     * @throws IOException if any IO error, related to off-heap memory allocation or file mapping,
     * or establishing replication connections, occurs
     * @throws IllegalStateException if {@code create()} method has already been called on this
     * {@code ChronicleHashInstanceBuilder}
     * @see ChronicleHashBuilder#create()
     * @see ChronicleHashBuilder#createPersistedTo(File)
     */
    H create() throws IOException;
}
