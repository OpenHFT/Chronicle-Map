/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.map.TcpReplicator;
import net.openhft.chronicle.map.UdpReplicator;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public final class ChannelProviderBuilder {

    byte identifier;
    int maxEntrySize = 1024;
    int maxNumberOfChronicles = 128;
    private UdpReplicationConfig udpReplicationConfig = null;
    private TcpReplicationConfig tcpReplicationConfig = null;



    public int maxEntrySize() {
        return maxEntrySize;
    }

    public ChannelProviderBuilder maxEntrySize(int maxEntrySize) {
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public ChannelProviderBuilder replicators(byte identifier, ReplicationConfig... replicationConfigs) {
        this.identifier = identifier;


        for (ReplicationConfig replicationConfig : replicationConfigs) {

            if (replicationConfig instanceof TcpReplicationConfig) {
                this.tcpReplicationConfig = (TcpReplicationConfig) replicationConfig;
            } else if (replicationConfig instanceof UdpReplicationConfig) {
                this.udpReplicationConfig = (UdpReplicationConfig) replicationConfig;
            } else
                throw new UnsupportedOperationException();
        }
        return this;
    }

    public ChannelProviderBuilder maxNumberOfChronicles(int maxNumberOfChronicles) {
        this.maxNumberOfChronicles = maxNumberOfChronicles;
        return this;
    }

    public ChannelProvider create() throws IOException {
        final ChannelProvider replicatingCluster = new ChannelProvider(this);
        if (tcpReplicationConfig != null) {
            final TcpReplicator tcpReplicator = new TcpReplicator(
                    replicatingCluster.asReplica,
                    replicatingCluster.asEntryExternalizable,
                    tcpReplicationConfig,
                    maxEntrySize, null);
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
