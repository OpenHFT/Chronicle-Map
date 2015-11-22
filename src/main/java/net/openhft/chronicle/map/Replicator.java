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

import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.UdpTransportConfig;

import java.io.Closeable;
import java.io.IOException;

interface Replicator {

    String ONLY_UDP_WARN_MESSAGE =
            "MISSING TCP REPLICATION : The UdpReplicator only attempts to read data " +
                    "(it does not enforce or guarantee delivery), you should use" +
                    "the UdpReplicator if you have a large number of nodes, and you wish" +
                    "to receive the data before it becomes available on TCP/IP. Since data" +
                    "delivery is not guaranteed, it is recommended that you only use" +
                    "the UDP Replicator in conjunction with a TCP Replicator";

    static Replicator tcp(final AbstractReplication replication) {
        return (builder, replica, entryExternalizable, replicatedMap) ->
                new TcpReplicator(replica, entryExternalizable,
                        replication.tcpTransportAndNetwork(), replication.remoteNodeValidator(),
                        replication.name(), replication.connectionListener());
    }

    static Replicator udp(final UdpTransportConfig replicationConfig) {
        return (builder, map, entryExternalizable, replicatedMap) ->
                new UdpReplicator(map, entryExternalizable, replicationConfig);
    }

    /**
     * Applies the replicator to the map instance and returns a Closeable token to manage resources,
     * associated with the replication.  <p>This method isn't intended to be called from the client
     * code.
     *
     * @param builder             the builder from which the map was constructed. The replicator may
     *                            obtain some map configurations, not accessible via the map
     *                            instance.
     * @param map                 a replicated map instance. Provides basic tools for replication
     *                            implementation.
     * @param entryExternalizable the callback for ser/deser implementation in the replicator
     * @return a {@code Closeable} token to control replication resources. It should be closed on
     * closing the replicated map.
     * @throws java.io.IOException   if an io error occurred during the replicator setup
     * @throws IllegalStateException if this replicator doesn't support application to more than one
     *                               map (or the specified number of maps), and this replicator has
     *                               already been applied to a map (or the specified number of
     *                               maps)
     */
     Closeable applyTo(
             ChronicleMapBuilder builder, Replica map,
             Replica.EntryExternalizable entryExternalizable, ReplicatedChronicleMap replicatedMap)
             throws IOException;
}
