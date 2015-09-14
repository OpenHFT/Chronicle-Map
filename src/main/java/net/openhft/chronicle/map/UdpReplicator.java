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

import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * The UdpReplicator attempts to read the data ( but it does not enforce or grantee delivery ),
 * typically, you should use the UdpReplicator if you have a large number of nodes, and you wish to
 * receive the data before it becomes available on TCP/IP. In order to not miss data, UdpReplicator
 * should be used in conjunction with the TCP Replicator.
 */
final class UdpReplicator extends UdpChannelReplicator implements Replica.ModificationNotifier, Closeable {

    public static final int UPD_BUFFER_SIZE = 64 * 1204;
    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    public UdpReplicator(@NotNull final Replica replica,
                         @NotNull final Replica.EntryExternalizable entryExternalizable,
                         @NotNull final UdpTransportConfig replicationConfig) throws IOException {

        super(replicationConfig, replica.identifier());

        Replica.ModificationIterator modificationIterator = replica.acquireModificationIterator(
                ChronicleMapBuilder.UDP_REPLICATION_MODIFICATION_ITERATOR_ID);
        modificationIterator.setModificationNotifier(this);
        setReader(new UdpSocketChannelEntryReader(replicationConfig.udpBufferSize(), entryExternalizable));

        setWriter(new UdpSocketChannelEntryWriter(replicationConfig.udpBufferSize(),
                entryExternalizable,
                modificationIterator, this, new Replicators.OutBuffer(UPD_BUFFER_SIZE)));

        start();
    }

    private static class UdpSocketChannelEntryWriter implements EntryWriter {

        private final EntryCallback entryCallback;
        private final UdpChannelReplicator udpReplicator;
        private final Replicators.OutBuffer outBuffer;
        private Replica.ModificationIterator modificationIterator;

        UdpSocketChannelEntryWriter(final int updBufferSize,
                                    @NotNull final Replica.EntryExternalizable externalizable,
                                    @NotNull final Replica.ModificationIterator modificationIterator,
                                    UdpChannelReplicator udpReplicator, Replicators.OutBuffer outBuffer) {
            this.udpReplicator = udpReplicator;

            this.outBuffer = outBuffer;
            // we make the buffer twice as large just to give ourselves headroom

            entryCallback = new EntryCallback(externalizable, updBufferSize);
            this.modificationIterator = modificationIterator;
        }

        /**
         * writes all the entries that have changed, to the tcp socket
         *
         * @param socketChannel
         * @param modificationIterator
         * @throws InterruptedException
         * @throws java.io.IOException
         */
        /**
         * update that are throttled are rejected.
         *
         * @param socketChannel the socketChannel that we will write to
         * @throws InterruptedException
         * @throws IOException
         */
        public int writeAll(@NotNull final DatagramChannel socketChannel)
                throws InterruptedException, IOException {

            final ByteBufferBytes in = outBuffer.in();
            final ByteBuffer out = outBuffer.out();

            out.clear();
            in.clear();
            in.skip(SIZE_OF_SIZE);

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback, 0);

            if (!wasDataRead) {
                udpReplicator.disableWrites();
                return 0;
            }

            // we'll write the size inverted at the start
            in.writeShort(0, ~(in.readUnsignedShort(SIZE_OF_SIZE)));
            out.limit((int) in.position());

            return socketChannel.write(out);
        }
    }

    private class UdpSocketChannelEntryReader implements EntryReader {

        private final Replica.EntryExternalizable externalizable;
        private final ByteBuffer in;
        private final ByteBufferBytes out;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        UdpSocketChannelEntryReader(final int serializedEntrySize,
                                    @NotNull final Replica.EntryExternalizable externalizable) {
            // we make the buffer twice as large just to give ourselves headroom
            in = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            this.externalizable = externalizable;
            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
        }

        /**
         * reads entries from the socket till it is empty
         *
         * @param socketChannel the socketChannel that we will read from
         * @throws IOException
         * @throws InterruptedException
         */
        public void readAll(@NotNull final DatagramChannel socketChannel) throws IOException {

            out.clear();
            in.clear();

            socketChannel.receive(in);

            final int bytesRead = in.position();

            if (bytesRead < SIZE_OF_SIZE + SIZE_OF_SIZE)
                return;

            out.limit(in.position());

            final int invertedSize = out.readInt();
            final int size = out.readInt();

            // check the the first 4 bytes are the inverted len followed by the len
            // we do this to check that this is a valid start of entry, otherwise we throw it away
            if (~size != invertedSize)
                return;

            if (out.remaining() != size)
                return;

            externalizable.readExternalEntry(copies, segmentState, out);
        }
    }

}

