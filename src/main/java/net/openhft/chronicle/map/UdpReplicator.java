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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.UdpReplicationConfig;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;


/**
 * The UdpReplicator attempts to read the data ( but it does not enforce or grantee delivery ), typically, you
 * should use the UdpReplicator if you have a large number of nodes, and you wish to receive the data before
 * it becomes available on TCP/IP. In order to not miss data, UdpReplicator should be used in conjunction with
 * the TCP Replicator.
 */
public class UdpReplicator extends UdpChannelReplicator implements Replica.ModificationNotifier, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    public UdpReplicator(@NotNull final Replica replica,
                         @NotNull final Replica.EntryExternalizable entryExternalizable,
                         @NotNull final UdpReplicationConfig replicationConfig,
                         final int serializedEntrySize)
            throws IOException {

        super(replicationConfig, serializedEntrySize, replica.identifier());

        Replica.ModificationIterator modificationIterator = replica.acquireModificationIterator(
                AbstractChronicleMapBuilder.UDP_REPLICATION_MODIFICATION_ITERATOR_ID, this);

        setReader(new UdpSocketChannelEntryReader(serializedEntrySize, entryExternalizable));

        setWriter(new UdpSocketChannelEntryWriter(serializedEntrySize, entryExternalizable,
                modificationIterator, this));

        start();
    }

    private static class UdpSocketChannelEntryReader implements EntryReader {

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
        public void readAll(@NotNull final DatagramChannel socketChannel) throws IOException,
                InterruptedException {

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
            if ( ~size != invertedSize)
                return;

            if (out.remaining() != size)
                return;

            externalizable.readExternalEntry(out);
        }

    }

    private static class UdpSocketChannelEntryWriter implements EntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;
        private final UdpChannelReplicator udpReplicator;
        private Replica.ModificationIterator modificationIterator;

        UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final Replica.EntryExternalizable externalizable,
                                    @NotNull final Replica.ModificationIterator modificationIterator,
                                    UdpChannelReplicator udpReplicator) {
            this.udpReplicator = udpReplicator;

            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);
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


}





