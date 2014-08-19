/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Math.min;
import static java.nio.ByteBuffer.wrap;
import static net.openhft.collections.Replica.ModificationNotifier.NOP;

/**
 * @author Rob Austin.
 */
class ClusterReplicator<K, V> implements ReplicaExternalizable<K, V>, Closeable {

    public static final int WRITE_BUFFER_SIZE = 1024;
    private final byte localIdentifer;

    interface MessageHandler {
        void onMessage(Bytes bytes);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ClusterReplicator.class.getName());

    private final byte BOOTSTAP_MESSAGE = 'B';

    private final DirectBitSet chronicalChannelBitSet;
    private final ReplicaExternalizable chronicalChannels[];

    private final AtomicReferenceArray<PayloadProvider> systemModificationIterator = new
            AtomicReferenceArray<PayloadProvider>(128);

    private final DirectBitSet systemModificationIteratorBitSet = newBitSet(systemModificationIterator.length());

    private final AtomicReferenceArray<ModificationIterator> modificationIterator = new
            AtomicReferenceArray<ModificationIterator>(128);

    private final Set<AbstractChannelReplicator> replicators = new CopyOnWriteArraySet<AbstractChannelReplicator>();

    /**
     * handles a message received via the SystemQueue
     */
    private final MessageHandler systemMessageHandler = new MessageHandler() {

        @Override
        public void onMessage(Bytes bytes) {
            final byte type = bytes.readByte();

            if (type == BOOTSTAP_MESSAGE) {
                onBootstrapMessage(bytes);
            } else {
                LOG.info("message of type=" + type + " was ignored.");
            }

        }

    };

    /**
     * used to send an receive system messages
     */
    private final SystemQueue systemMessageQueue;

    public ClusterReplicator(final byte identifier, int maxNumberOfChronicleChannels) {
        this.localIdentifer = identifier;

        chronicalChannels = new ReplicaExternalizable[maxNumberOfChronicleChannels];
        chronicalChannelBitSet = newBitSet(chronicalChannels.length);

        systemMessageQueue = new SystemQueue(systemModificationIteratorBitSet, systemModificationIterator, systemMessageHandler);
        add((short) 0, systemMessageQueue.replicaExternalizable);
    }


    /**
     * called whenever we receive a bootstrap message
     *
     * @param bytes
     */
    private void onBootstrapMessage(Bytes bytes) {
        final short remoteIdentifier = bytes.readByte();
        final int chronicleChannel = bytes.readUnsignedShort();
        final long lastModificationTime = bytes.readLong();

        // this could be null if once node has a chronicle channel before the other
        if (chronicalChannels[chronicleChannel] != null)
            chronicalChannels[chronicleChannel].acquireModificationIterator(remoteIdentifier, NOP).dirtyEntries(lastModificationTime);

    }

    private ByteBufferBytes toBootstrapMessage(short chronicleChannel, final long lastModificationTime) {
        final ByteBufferBytes writeBuffer = new ByteBufferBytes(ByteBuffer.allocate(1 + 1 + 2 + 8));
        writeBuffer.writeByte(BOOTSTAP_MESSAGE);
        writeBuffer.writeByte(localIdentifer);
        writeBuffer.writeUnsignedShort(chronicleChannel);
        writeBuffer.writeLong(lastModificationTime);
        writeBuffer.flip();
        return writeBuffer;
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

    public void add(short chronicleChannel, ReplicaExternalizable replica) {

        if (chronicalChannels[chronicleChannel] != null)
            throw new IllegalArgumentException("chronicleId=" + chronicleChannel + " is already in use.");

        chronicalChannels[chronicleChannel] = replica;
        chronicalChannelBitSet.set(chronicleChannel);


        // zero is used for bootstrapping
        if (chronicleChannel == 0)
            return;


        // boot strap messages are only sent if the cluster is already established
        for (int i = (int) systemModificationIteratorBitSet.nextSetBit(0); i > 0; i = (int) systemModificationIteratorBitSet.nextSetBit(i + 1)) {
            byte remoteIdentifier = (byte) i;
            final long lastModificationTime = replica.lastModificationTime(remoteIdentifier);
            final ByteBufferBytes message = toBootstrapMessage(chronicleChannel, lastModificationTime);
            systemModificationIterator.get(remoteIdentifier).addPayload(message);
        }

    }


    /**
     * writes the entry to the chronicle channel provided
     *
     * @param entry            the byte location of the entry to be stored
     * @param destination      a buffer the entry will be written to, the segment may reject this operation
     *                         and add zeroBytes, if the identifier in the entry did not match the maps local
     * @param chronicleChannel used in cluster into identify the canonical map or queue
     */
    @Override
    public void writeExternalEntry(@NotNull AbstractBytes entry, @NotNull Bytes destination, int chronicleChannel) {
        destination.writeStopBit(chronicleChannel);
        chronicalChannels[chronicleChannel].writeExternalEntry(entry, destination, chronicleChannel);
    }

    @Override
    public void readExternalEntry(@NotNull Bytes source) {
        final int chronicleId = (int) source.readStopBit();
        if (chronicleId < chronicalChannels.length)
            chronicalChannels[chronicleId].readExternalEntry(source);
        else
            LOG.info("skipped entry with chronicleId=" + chronicleId + ", ");
    }

    @Override
    public byte identifier() {
        return this.localIdentifer;
    }

    @Override
    public ModificationIterator acquireModificationIterator(final short remoteIdentifier,
                                                            final ModificationNotifier notifier) {
        final ModificationIterator result = modificationIterator.get(remoteIdentifier);
        if (result != null)
            return result;

        final ModificationIterator result0 = new ModificationIterator() {

            @Override
            public boolean hasNext() {
                for (int i = (int) chronicalChannelBitSet.nextSetBit(0); i >= 0; i = (int)
                        chronicalChannelBitSet.nextSetBit(i + 1)) {
                    final ModificationIterator modificationIterator = chronicalChannels[i]
                            .acquireModificationIterator(remoteIdentifier, notifier);
                    if (modificationIterator.hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public boolean nextEntry(@NotNull AbstractEntryCallback callback, final int na) {
                for (int i = (int) chronicalChannelBitSet.nextSetBit(0); i >= 0; i = (int)
                        chronicalChannelBitSet.nextSetBit(i + 1)) {
                    final ModificationIterator modificationIterator = chronicalChannels[i]
                            .acquireModificationIterator(remoteIdentifier, notifier);
                    if (modificationIterator.nextEntry(callback, i))
                        return true;
                }
                return false;
            }

            @Override
            public void dirtyEntries(long fromTimeStamp) {
                for (int i = (int) chronicalChannelBitSet.nextSetBit(0); i >= 0; i = (int)
                        chronicalChannelBitSet.nextSetBit(i + 1)) {
                    chronicalChannels[i].acquireModificationIterator(remoteIdentifier,
                            notifier).dirtyEntries(fromTimeStamp);
                }
                notifier.onChange();
            }
        };

        modificationIterator.set((int) remoteIdentifier, result0);
        return result0;

    }


    /**
     * gets the earliest modification time for all of the chronicles
     *
     * @param remoteIdentifier the remote identifier
     * @return
     */
    @Override
    public long lastModificationTime(byte remoteIdentifier) {

        // if isEmpty, then we will ask for all the data from the remote node, as there could be a race
        // around the chronicalChannelBitSet and the bootstrap message
        if (chronicalChannelBitSet.nextSetBit(0) == -1) {
            return 0;
        }

        long t = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(1);
        for (int i = (int) chronicalChannelBitSet.nextSetBit(0); i > 0; i = (int) chronicalChannelBitSet.nextSetBit(i + 1)) {
            t = min(t, chronicalChannels[i].lastModificationTime(remoteIdentifier));
        }
        return t;
    }

    @Override
    public void close() throws IOException {

        for (AbstractChannelReplicator replicator : replicators) {
            replicator.close();
        }

        for (int i = (int) chronicalChannelBitSet.nextSetBit(0); i > 0; i = (int) chronicalChannelBitSet.nextSetBit(i + 1)) {
            chronicalChannels[i].close();
        }
    }


    public void add(AbstractChannelReplicator replicator) {
        replicators.add(replicator);
    }


    interface PayloadProvider extends ModificationIterator {
        void addPayload(final Bytes bytes);
    }


    /**
     * used to send system messages such as bootstrap from one remote node to another, it also can be used in
     * a broadcast context
     */
    static class SystemQueue {


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


        final ReplicaExternalizable replicaExternalizable = new ReplicaExternalizable() {

            @Override
            public byte identifier() {
                return 0;
            }

            @Override
            public ModificationIterator acquireModificationIterator(final short remoteIdentifier,
                                                                    final ModificationNotifier modificationNotifier) {

                final ModificationIterator result = systemModificationIterator.get(remoteIdentifier);

                if (result != null)
                    return result;

                final PayloadProvider iterator = new PayloadProvider() {

                    @Override
                    public boolean hasNext() {
                        return payloads.peek() != null;
                    }

                    final ByteBufferBytes writeBuffer = new ByteBufferBytes(ByteBuffer.allocate(WRITE_BUFFER_SIZE));

                    @Override
                    public boolean nextEntry(@NotNull AbstractEntryCallback callback, int na) {

                        final Bytes bytes = payloads.poll();
                        if (bytes == null)
                            return false;

                        if (bytes instanceof AbstractBytes)
                            callback.onEntry((AbstractBytes) bytes, 0);
                        else {
                            writeBuffer.clear();
                            writeBuffer.write(bytes);
                            callback.onEntry(writeBuffer, 0);
                        }

                        return true;
                    }

                    @Override
                    public void dirtyEntries(long fromTimeStamp) {
                        // do nothing
                    }

                    final Queue<Bytes> payloads = new LinkedTransferQueue<Bytes>();

                    @Override
                    public void addPayload(Bytes bytes) {

                        if (bytes.remaining() == 0)
                            return;

                        if (bytes.remaining() > writeBuffer.capacity())
                            throw new IllegalArgumentException("BUFFER_OVERFLOW: bytes.remaining()=" + bytes
                                    .remaining() + " " +
                                    " the maximum allow number of bytes is " + writeBuffer.capacity());

                        payloads.add(bytes);

                        // notifies that a change has been made, this will nudge the OP_WRITE selector to
                        // push this update out over the nio socket
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

            @Override
            public void writeExternalEntry(@NotNull AbstractBytes entry, @NotNull Bytes destination, int na) {
                destination.write(entry);
            }

            @Override
            public void readExternalEntry(@NotNull Bytes source) {
                messageHandler.onMessage(source);
            }
        };
    }
}