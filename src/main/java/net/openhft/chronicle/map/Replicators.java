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

import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

final class Replicators {

    static final String ONLY_UDP_WARN_MESSAGE =
            "MISSING TCP REPLICATION : The UdpReplicator only attempts to read data " +
            "(it does not enforce or guarantee delivery), you should use" +
            "the UdpReplicator if you have a large number of nodes, and you wish" +
            "to receive the data before it becomes available on TCP/IP. Since data" +
            "delivery is not guaranteed, it is recommended that you only use" +
            "the UDP Replicator in conjunction with a TCP Replicator";

    private Replicators() {
    }

    static Replicator tcp(final AbstractReplication replication) {
        return new Replicator() {

            @Override
            protected Closeable applyTo(@NotNull final AbstractChronicleMapBuilder builder,
                                        @NotNull final Replica replica,
                                        @NotNull final Replica.EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap)
                    throws IOException {

                StatelessServerConnector statelessServer = new StatelessServerConnector(
                        builder.keyBuilder, builder.valueBuilder,
                                (VanillaChronicleMap) chronicleMap, builder.entrySize(true));

                TcpTransportAndNetworkConfig tcpConfig = replication.tcpTransportAndNetwork();
                assert tcpConfig != null;
                return new TcpReplicator(replica, entryExternalizable,
                        tcpConfig,
                        builder.entrySize(true), statelessServer,
                        replication.remoteNodeValidator());
            }
        };
    }

    static Replicator udp(
            final UdpTransportConfig replicationConfig) {
        return new Replicator() {

            @Override
            protected Closeable applyTo(@NotNull final AbstractChronicleMapBuilder builder,
                                        @NotNull final Replica map,
                                        @NotNull final Replica.EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap)
                    throws IOException {
                return new UdpReplicator(map, entryExternalizable, replicationConfig,
                        builder.entrySize(true));
            }
        };
    }
}
