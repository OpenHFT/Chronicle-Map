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


import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Math.min;
import static java.nio.ByteBuffer.wrap;
import static net.openhft.chronicle.map.Replica.EntryExternalizable;
import static net.openhft.chronicle.map.Replica.ModificationIterator;
import static net.openhft.chronicle.map.Replica.ModificationNotifier.NOP;

/**
 * @author Rob Austin.
 */
final class ChannelProvider implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelProvider.class.getName());

    static final Map<ReplicationHub, ChannelProvider> implMapping = new IdentityHashMap<>();

    static synchronized ChannelProvider getProvider(ReplicationHub hub) throws IOException {
        ChannelProvider channelProvider = implMapping.get(hub);
        if (channelProvider != null)
            return channelProvider;
        channelProvider = new ChannelProvider(hub);
        TcpTransportAndNetworkConfig tcpConfig = hub.tcpTransportAndNetwork();
        if (tcpConfig != null) {
            final TcpReplicator tcpReplicator = new TcpReplicator(
                    channelProvider.asReplica,
                    channelProvider.asEntryExternalizable,
                    tcpConfig, hub.maxEntrySize(), null, hub.remoteNodeValidator());
            channelProvider.add(tcpReplicator);
        }

        UdpTransportConfig udpConfig = hub.udpTransport();
        if (udpConfig != null) {
            final UdpReplicator udpReplicator =
                    new UdpReplicator(
                            channelProvider.asReplica,
                            channelProvider.asEntryExternalizable,
                            udpConfig, hub.maxEntrySize());
            channelProvider.add(udpReplicator);
            if (tcpConfig == null)
                LOG.warn(Replicators.ONLY_UDP_WARN_MESSAGE);
        }
        implMapping.put(hub, channelProvider);
        return channelProvider;
    }

    private static final byte BOOTSTRAP_MESSAGE = 'B';
    final EntryExternalizable asEntryExternalizable = new EntryExternalizable() {
        /**
         * writes the entry to the chronicle channel provided
         *
         * @param entry            the byte location of the entry to be stored
         * @param destination      a buffer the entry will be written to, the segment may reject
         *                         this operation and add zeroBytes, if the identifier in the entry
         *                         did not match the maps local
         * @param chronicleChannel used in cluster into identify the canonical map or queue
         */
        @Override
        public void writeExternalEntry(@NotNull Bytes entry, @NotNull Bytes destination,
                                       int chronicleChannel) {
            channelDataLock.readLock().lock();
            try {
                destination.writeStopBit(chronicleChannel);
                channelEntryExternalizables[chronicleChannel]
                        .writeExternalEntry(entry, destination, chronicleChannel);
            } finally {
                channelDataLock.readLock().unlock();
            }
        }

        @Override
        public void readExternalEntry(@NotNull Bytes source) {
            channelDataLock.readLock().lock();
            try {
                final int chronicleId = (int) source.readStopBit();
                if (chronicleId < chronicleChannels.length)
                    channelEntryExternalizables[chronicleId].readExternalEntry(source);
                else
                    LOG.info("skipped entry with chronicleId=" + chronicleId + ", ");
            } finally {
                channelDataLock.readLock().unlock();
            }
        }
    };
    private final byte localIdentifier;
    final Replica asReplica = new Replica() {

        @Override
        public byte identifier() {
            return localIdentifier;
        }

        @Override
        public ModificationIterator acquireModificationIterator(
                final short remoteIdentifier, final ModificationNotifier notifier) {
            channelDataLock.writeLock().lock();
            try {
                final ModificationIterator result = modificationIterator.get(remoteIdentifier);
                if (result != null)
                    return result;

                final ModificationIterator result0 = new ModificationIterator() {

                    @Override
                    public boolean hasNext() {
                        channelDataLock.readLock().lock();
                        try {
                            // old-style iteration to avoid 1) iterator object creation
                            // 2) ConcurrentModificationException
                            for (int i = 0, len = chronicleChannelList.size(); i < len; i++) {
                                final ModificationIterator modificationIterator =
                                        chronicleChannelList.get(i).acquireModificationIterator(
                                                remoteIdentifier, notifier);
                                if (modificationIterator.hasNext())
                                    return true;
                            }
                            return false;
                        } finally {
                            channelDataLock.readLock().unlock();
                        }
                    }

                    @Override
                    public boolean nextEntry(@NotNull EntryCallback callback,
                                             final int na) {
                        channelDataLock.readLock().lock();
                        try {
                            for (int i = 0, len = chronicleChannelList.size(); i < len; i++) {
                                Replica chronicleChannel = chronicleChannelList.get(i);
                                final ModificationIterator modificationIterator = chronicleChannel
                                        .acquireModificationIterator(remoteIdentifier, notifier);
                                if (modificationIterator
                                        .nextEntry(callback, chronicleChannelIds.get(i)))
                                    return true;
                            }
                            return false;
                        } finally {
                            channelDataLock.readLock().unlock();
                        }
                    }

                    @Override
                    public void dirtyEntries(long fromTimeStamp) {
                        channelDataLock.readLock().lock();
                        try {
                            for (int i = 0, len = chronicleChannelList.size(); i < len; i++) {
                                chronicleChannelList.get(i)
                                        .acquireModificationIterator(remoteIdentifier, notifier)
                                        .dirtyEntries(fromTimeStamp);
                                notifier.onChange();
                            }
                        } finally {
                            channelDataLock.readLock().unlock();
                        }
                    }
                };

                modificationIterator.set((int) remoteIdentifier, result0);
                return result0;
            } finally {
                channelDataLock.writeLock().unlock();
            }
        }

        /**
         * gets the earliest modification time for all of the chronicles
         */
        @Override
        public long lastModificationTime(byte remoteIdentifier) {
            channelDataLock.readLock().lock();
            try {
                long t = 0;
                // not including the SystemQueue at index 0
                for (int i = 1, len = chronicleChannelList.size(); i < len; i++) {
                    Replica channel = chronicleChannelList.get(i);
                    t = (t == 0) ? channel.lastModificationTime(remoteIdentifier) :
                            min(t, channel.lastModificationTime(remoteIdentifier));
                }
                return t;
            } finally {
                channelDataLock.readLock().unlock();
            }
        }

        @Override
        public void close() throws IOException {
            ChannelProvider.this.close();
        }
    };

    private final ReplicationHub hub;
    private final int maxEntrySize;

    private final ReadWriteLock channelDataLock = new ReentrantReadWriteLock();

    // start of channel data
    private final Replica[] chronicleChannels;
    private final List<Replica> chronicleChannelList;
    private final List<Integer> chronicleChannelIds;
    private final EntryExternalizable[] channelEntryExternalizables;
    private final AtomicReferenceArray<PayloadProvider> systemModificationIterator =
            new AtomicReferenceArray<PayloadProvider>(128);
    private final DirectBitSet systemModificationIteratorBitSet =
            newBitSet(systemModificationIterator.length());
    private final AtomicReferenceArray<ModificationIterator> modificationIterator =
            new AtomicReferenceArray<ModificationIterator>(128);
    // end of channel data

    private final Set<Closeable> replicators = new CopyOnWriteArraySet<Closeable>();

    private volatile boolean isClosed = false;

    private ChannelProvider(ReplicationHub hub) {
        localIdentifier = hub.identifier();
        this.hub = hub;
        maxEntrySize = hub.maxEntrySize();
        chronicleChannels = new Replica[hub.maxNumberOfChannels()];
        channelEntryExternalizables = new EntryExternalizable[hub.maxNumberOfChannels()];
        chronicleChannelList = new ArrayList<Replica>();
        chronicleChannelIds = new ArrayList<Integer>();
        MessageHandler systemMessageHandler = new MessageHandler() {
            @Override
            public void onMessage(Bytes bytes) {
                final byte type = bytes.readByte();
                if (type == BOOTSTRAP_MESSAGE) {
                    onBootstrapMessage(bytes);
                } else {
                    LOG.info("message of type=" + type + " was ignored.");
                }
            }
        };
        SystemQueue systemMessageQueue = new SystemQueue(
                systemModificationIteratorBitSet, systemModificationIterator, systemMessageHandler);
        add((short) 0, systemMessageQueue.asReplica, systemMessageQueue.asEntryExternalizable);
    }

    public int maxEntrySize() {
        return maxEntrySize;
    }

    /**
     * creates a bit set based on a number of bits
     *
     * @param numberOfBits the number of bits the bit set should include
     * @return a new DirectBitSet backed by a byteBuffer
     */
    private static DirectBitSet newBitSet(int numberOfBits) {
        final ByteBufferBytes bytes = new ByteBufferBytes(wrap(new byte[(numberOfBits + 7) / 8]));
        return new SingleThreadedDirectBitSet(bytes);
    }

    public ChronicleChannel createChannel(int channel) {
        return new ChronicleChannel(channel);
    }

    /**
     * called whenever we receive a bootstrap message
     */
    private void onBootstrapMessage(Bytes bytes) {
        final short remoteIdentifier = bytes.readByte();
        final int chronicleChannel = bytes.readUnsignedShort();
        final long lastModificationTime = bytes.readLong();

        // this could be null if one node has a chronicle channel before the other
        if (chronicleChannels[chronicleChannel] != null) {
            chronicleChannels[chronicleChannel].acquireModificationIterator(remoteIdentifier, NOP)
                    .dirtyEntries(lastModificationTime);
        }
    }

    private ByteBufferBytes toBootstrapMessage(int chronicleChannel, final long lastModificationTime) {
        final ByteBufferBytes writeBuffer = new ByteBufferBytes(ByteBuffer.allocate(1 + 1 + 2 + 8));
        writeBuffer.writeByte(BOOTSTRAP_MESSAGE);
        writeBuffer.writeByte(localIdentifier);
        writeBuffer.writeUnsignedShort(chronicleChannel);
        writeBuffer.writeLong(lastModificationTime);
        writeBuffer.flip();
        return writeBuffer;
    }

    private void add(int chronicleChannel,
                     Replica replica, EntryExternalizable entryExternalizable) {
        channelDataLock.writeLock().lock();
        try {
            if (chronicleChannels[chronicleChannel] != null) {
                throw new IllegalStateException("chronicleId=" + chronicleChannel +
                        " is already in use.");
            }
            chronicleChannels[chronicleChannel] = replica;
            chronicleChannelList.add(replica);
            chronicleChannelIds.add((int) chronicleChannel);
            channelEntryExternalizables[chronicleChannel] = entryExternalizable;

            if (chronicleChannel == 0)
                return;

            for (int i = (int) systemModificationIteratorBitSet.nextSetBit(0); i > 0;
                 i = (int) systemModificationIteratorBitSet.nextSetBit(i + 1)) {
                byte remoteIdentifier = (byte) i;
                final long lastModificationTime = replica.lastModificationTime(remoteIdentifier);
                final ByteBufferBytes message =
                        toBootstrapMessage(chronicleChannel, lastModificationTime);
                systemModificationIterator.get(remoteIdentifier).addPayload(message);
            }
        } finally {
            channelDataLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed)
            return;
        // to be synchronized with #getProvider()
        synchronized (ChannelProvider.class) {
            isClosed = true;
            for (Closeable replicator : replicators) {
                try {
                    replicator.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }

            implMapping.remove(hub);
        }
    }

    void add(Closeable replicator) {
        replicators.add(replicator);
    }

    private interface MessageHandler {
        void onMessage(Bytes bytes);
    }

    private interface PayloadProvider extends ModificationIterator {
        void addPayload(final Bytes bytes);
    }

    /**
     * used to send system messages such as bootstrap from one remote node to another, it also can be used in
     * a broadcast context
     */
    static class SystemQueue {


        final Replica asReplica = new Replica() {

            @Override
            public byte identifier() {
                return 0;
            }

            @Override
            public ModificationIterator acquireModificationIterator(
                    final short remoteIdentifier, final ModificationNotifier modificationNotifier) {

                final ModificationIterator result = systemModificationIterator.get(remoteIdentifier);

                if (result != null)
                    return result;

                final PayloadProvider iterator = new PayloadProvider() {


                    // todo it maybe possibel to replace this with a single atomic refernce,
                    // todo this way no aditional objects will be created
                    final Queue<Bytes> payloads = new LinkedTransferQueue<Bytes>();

                    @Override
                    public boolean hasNext() {
                        return payloads.peek() != null;
                    }

                    @Override
                    public boolean nextEntry(@NotNull EntryCallback callback, int na) {
                        final Bytes bytes = payloads.poll();
                        if (bytes == null)
                            return false;
                        callback.onEntry(bytes, 0);
                        return true;
                    }

                    @Override
                    public void dirtyEntries(long fromTimeStamp) {
                        // do nothing
                    }

                    @Override
                    public void addPayload(Bytes bytes) {
                        if (bytes.remaining() == 0)
                            return;
                        payloads.add(bytes);
                        // notifies that a change has been made, this will nudge the OP_WRITE
                        // selector to push this update out over the nio socket
                        modificationNotifier.onChange();
                    }
                };

                systemModificationIterator.set(remoteIdentifier, iterator);
                systemModificationIteratorBitSet.set(remoteIdentifier);

                return iterator;

            }

            @Override
            public long lastModificationTime(byte remoteIdentifier) {
                return 0;
            }

            @Override
            public void close() throws IOException {
                // do nothing
            }
        };
        final EntryExternalizable asEntryExternalizable = new EntryExternalizable() {
            @Override
            public void writeExternalEntry(@NotNull Bytes entry, @NotNull Bytes destination,
                                           int na) {
                destination.write(entry);
            }

            @Override
            public void readExternalEntry(@NotNull Bytes source) {
                messageHandler.onMessage(source);
            }
        };
        private final DirectBitSet systemModificationIteratorBitSet;
        private final AtomicReferenceArray<PayloadProvider> systemModificationIterator;
        private final MessageHandler messageHandler;

        SystemQueue(DirectBitSet systemModificationIteratorBitSet,
                    AtomicReferenceArray<PayloadProvider> systemModificationIterator,
                    MessageHandler messageHandler) {

            this.systemModificationIteratorBitSet = systemModificationIteratorBitSet;
            this.systemModificationIterator = systemModificationIterator;
            this.messageHandler = messageHandler;
        }
    }

    public class ChronicleChannel extends Replicator implements Closeable {

        private final int chronicleChannel;

        private ChronicleChannel(int chronicleChannel) {
            this.chronicleChannel = chronicleChannel;
        }

        public byte identifier() {
            return localIdentifier;
        }

        @Override
        protected Closeable applyTo(AbstractChronicleMapBuilder builder,
                                    Replica map, EntryExternalizable entryExternalizable,
                                    final ChronicleMap chronicleMap) {
            add(chronicleChannel, map, entryExternalizable);
            return this;
        }

        @Override
        public void close() throws IOException {
            channelDataLock.writeLock().lock();
            try {
                chronicleChannelList.remove(chronicleChannels[chronicleChannel]);
                chronicleChannelIds.remove((Integer) (int) chronicleChannel);
                chronicleChannels[chronicleChannel] = null;
                channelEntryExternalizables[chronicleChannel] = null;

                if (chronicleChannelList.size() == 1) // i. e. only SystemQueue is left
                    ChannelProvider.this.close();
            } finally {
                channelDataLock.writeLock().unlock();
            }
        }

    }
}