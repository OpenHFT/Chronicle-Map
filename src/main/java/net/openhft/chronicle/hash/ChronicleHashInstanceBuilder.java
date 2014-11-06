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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.SimpleReplication;
import net.openhft.chronicle.hash.replication.TcpConfig;

import java.io.File;
import java.io.IOException;

public interface ChronicleHashInstanceBuilder<C extends ChronicleHash> {

    ChronicleHashInstanceBuilder<C> replicated(byte identifier, TcpConfig tcpTransportAndNetwork);

    ChronicleHashInstanceBuilder<C> replicated(SimpleReplication replication);

    /**
     * Configures replication of the hash containers, created by this builder, via so called
     * "channels". See
     * <a href="https://github.com/OpenHFT/Chronicle-Map#channels-and-channelprovider">the
     * section about Channels and ChannelProvider in ChronicleMap manual</a> for more information.
     *
     * <p>Another way to establish replication is {@link #replicated(SimpleReplication)} method
     * or it's shortcut: {@link #replicated(byte, TcpConfig)}.
     *
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     *
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by means of this method, {@link #replicated(SimpleReplication)}
     * or {@link #replicated(byte, TcpConfig)} method calls.
     *
     * @param channel the channel responsible for gathering updates of hash containers,
     *                created by this builder, and replicating them over network
     * @return this builder object back
     * @see #replicated(SimpleReplication)
     */
    ChronicleHashInstanceBuilder<C> replicatedViaChannel(ReplicationChannel channel);

    ChronicleHashInstanceBuilder<C> persistedTo(File file);

    C create() throws IOException;
}
