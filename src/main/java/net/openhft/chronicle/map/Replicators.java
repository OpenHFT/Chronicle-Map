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
                                        @NotNull final Replica.EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap) throws IOException {

                TcpTransportAndNetworkConfig tcpConfig = replication.tcpTransportAndNetwork();

                TcpReplicator.StatelessClientParameters statelessClientParameters =
                        new TcpReplicator.StatelessClientParameters(
                        (VanillaChronicleMap) chronicleMap,
                        builder.keyBuilder,
                        builder.valueBuilder);

                return new TcpReplicator(replica, entryExternalizable,
                        tcpConfig,
                        replication.remoteNodeValidator(), statelessClientParameters,
                        replication.name());
            }
        };
    }

    static Replicator udp(
            final UdpTransportConfig replicationConfig) {
        return new Replicator() {
            @Override
            protected Closeable applyTo(@NotNull final ChronicleMapBuilder builder,
                                        @NotNull final Replica map,
                                        @NotNull final Replica.EntryExternalizable entryExternalizable,
                                        final ChronicleMap chronicleMap)
                    throws IOException {
                return new UdpReplicator(map, entryExternalizable, replicationConfig
                );
            }
        };
    }
}
