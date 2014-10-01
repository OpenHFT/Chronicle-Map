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

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public final class ReplicatingClusterBuilder {

    final byte identifier;
    final int maxEntrySize;
    int maxNumberOfChronicles = 128;
    private UdpReplicationConfig udpReplicationConfig = null;
    private TcpReplicationConfig tcpReplicationConfig = null;

    ReplicatingClusterBuilder(byte identifier, final int maxEntrySize) {
        this.identifier = identifier;
        this.maxEntrySize = maxEntrySize;
        if (identifier <= 0) {
            throw new IllegalArgumentException("Identifier must be positive, identifier=" +
                    identifier);
        }
    }

    public ReplicatingClusterBuilder udpReplication(UdpReplicationConfig replicationConfig) {
        this.udpReplicationConfig = replicationConfig;
        return this;
    }

    public ReplicatingClusterBuilder tcpReplication(TcpReplicationConfig replicationConfig) {
        this.tcpReplicationConfig = replicationConfig;
        return this;
    }

    public ReplicatingClusterBuilder maxNumberOfChronicles(int maxNumberOfChronicles) {
        this.maxNumberOfChronicles = maxNumberOfChronicles;
        return this;
    }

    public ReplicatingCluster create() throws IOException {
        final ReplicatingCluster replicatingCluster = new ReplicatingCluster(this);
        if (tcpReplicationConfig != null) {
            final TcpReplicator tcpReplicator = new TcpReplicator(
                    replicatingCluster.asReplica,
                    replicatingCluster.asEntryExternalizable,
                    tcpReplicationConfig,
                    maxEntrySize);
            replicatingCluster.add(tcpReplicator);
        }

        if (udpReplicationConfig != null) {
            final UdpReplicator udpReplicator =
                    new UdpReplicator(replicatingCluster.asReplica,
                            replicatingCluster.asEntryExternalizable,
                            udpReplicationConfig,
                            maxEntrySize);
            replicatingCluster.add(udpReplicator);
        }
        return replicatingCluster;
    }
}
