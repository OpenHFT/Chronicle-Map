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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import net.openhft.chronicle.map.Replica.EntryExternalizable;
import net.openhft.chronicle.map.TcpReplicator.StatelessClientParameters;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

    static Replicator engineReplicaton(final AbstractReplication replication) {
        return new Replicator() {

            @Override
            protected Closeable applyTo(@NotNull final ChronicleMapBuilder builder,
                                        @NotNull final Replica replica,
                                        @NotNull final EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap) throws IOException {

                replication.engineReplicator().set(replica);

                return new Closeable() {

                    @Override
                    public void close() throws IOException {
                        // do nothing
                    }
                };
            }
        };
    }

    static class OutBuffer implements BufferResizer {

        @NotNull
        private ByteBufferBytes in;

        @NotNull
        private ByteBuffer out;

        @NotNull
        public ByteBufferBytes in() {
            return in;
        }

        @NotNull
        public ByteBuffer out() {
            return out;
        }


        OutBuffer(final int tcpBufferSize) {
            out = ByteBuffer.allocateDirect(tcpBufferSize);
            in = new ByteBufferBytes(out);
        }

        @Override
        public Bytes resizeBuffer(int newCapacity) {


            if (newCapacity < out.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            assert newCapacity < Integer.MAX_VALUE;

            final ByteBuffer result = ByteBuffer.allocate(newCapacity).order(ByteOrder.nativeOrder());
            final long bytesPosition = in.position();

            in = new ByteBufferBytes(result.slice());

            out.position(0);
            out.limit((int) bytesPosition);

            int numberOfLongs = (int) bytesPosition / 8;

            // chunk in longs first
            for (int i = 0; i < numberOfLongs; i++) {
                in.writeLong(out.getLong());
            }

            for (int i = numberOfLongs * 8; i < bytesPosition; i++) {
                in.writeByte(out.get());
            }

            out = result;

            assert out.capacity() == in.capacity();

            assert out.capacity() == newCapacity;
            assert out.capacity() == in.capacity();
            assert in.limit() == in.capacity();
            return in;

        }
    }

    static Replicator tcp(final AbstractReplication replication) {
        return new Replicator() {


            @Override
            protected Closeable applyTo(@NotNull final ChronicleMapBuilder builder,
                                        @NotNull final Replica replica,
                                        @NotNull final EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap) throws IOException {

                TcpTransportAndNetworkConfig tcpConfig = replication.tcpTransportAndNetwork();

                StatelessClientParameters statelessClientParameters =
                        new StatelessClientParameters(
                        (VanillaChronicleMap) chronicleMap,
                        builder.keyBuilder,
                        builder.valueBuilder);

                return new TcpReplicator(replica, entryExternalizable,
                        tcpConfig,
                        replication.remoteNodeValidator(),
                        statelessClientParameters,
                        replication.connectionListener());
            }
        };
    }

    static Replicator udp(
            final UdpTransportConfig replicationConfig) {
        return new Replicator() {
            @Override
            protected Closeable applyTo(@NotNull final ChronicleMapBuilder builder,
                                        @NotNull final Replica map,
                                        @NotNull final EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap)
                    throws IOException {
                return new UdpReplicator(map, entryExternalizable, replicationConfig
                );
            }
        };
    }
}
