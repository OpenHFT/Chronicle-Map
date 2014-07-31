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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.openhft.collections.Replica.EntryResolver;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;

/**
 * A Replicating Multi Master HashMap
 *
 * <p>Each remote hash map, mirrors its changes over to another remote hash map, neither hash map is
 * considered the master store of data, each hash map uses timestamps to reconcile changes. We refer to an
 * instance of a remote hash-map as a node. A node will be connected to any number of other nodes, for the
 * first implementation the maximum number of nodes will be fixed. The data that is stored locally in each
 * node will become eventually consistent. So changes made to one node, for example by calling put() will be
 * replicated over to the other node. To achieve a high level of performance and throughput, the call to put()
 * won’t block, with concurrentHashMap, It is typical to check the return code of some methods to obtain the
 * old value for example remove(). Due to the loose coupling and lock free nature of this multi master
 * implementation,  this return value will only be the old value on the nodes local data store. In other words
 * the nodes are only concurrent locally. Its worth realising that another node performing exactly the same
 * operation may return a different value. However reconciliation will ensure the maps themselves become
 * eventually consistent.
 *
 * <p>Reconciliation
 *
 * <p>If two ( or more nodes ) were to receive a change to their maps for the same key but different values,
 * say by a user of the maps, calling the put(key, value). Then, initially each node will update its local
 * store and each local store will hold a different value, but the aim of multi master replication is to
 * provide eventual consistency across the nodes. So, with multi master when ever a node is changed it will
 * notify the other nodes of its change. We will refer to this notification as an event. The event will hold a
 * timestamp indicating the time the change occurred, it will also hold the state transition, in this case it
 * was a put with a key and value. Eventual consistency is achieved by looking at the timestamp from the
 * remote node, if for a given key, the remote nodes timestamp is newer than the local nodes timestamp, then
 * the event from the remote node will be applied to the local node, otherwise the event will be ignored.
 *
 * <p>However there is an edge case that we have to concern ourselves with, If two nodes update their map at
 * the same time with different values, we have to deterministically resolve which update wins, because of
 * eventual consistency both nodes should end up locally holding the same data. Although it is rare two remote
 * nodes could receive an update to their maps at exactly the same time for the same key, we have to handle
 * this edge case, its therefore important not to rely on timestamps alone to reconcile the updates. Typically
 * the update with the newest timestamp should win, but in this example both timestamps are the same, and the
 * decision made to one node should be identical to the decision made to the other. We resolve this simple
 * dilemma by using a node identifier, each node will have a unique identifier, the update from the node with
 * the smallest identifier wins.
 *
 * @param <K> the entries key type
 * @param <V> the entries value type
 */
class VanillaSharedReplicatedHashMap<K, V> extends AbstractVanillaSharedHashMap<K, V>
        implements ChronicleMap<K, V>, ReplicaExternalizable<K, V>, EntryResolver<K, V>,
        Closeable {

    static final int MAX_UNSIGNED_SHORT = Character.MAX_VALUE;

    private static final Logger LOG = LoggerFactory.getLogger(VanillaSharedReplicatedHashMap.class);
    private static final int LAST_UPDATED_HEADER_SIZE = (127 * 8);

    // for file, jdbc and UDP replication
    public static final int RESERVED_MOD_ITER = 8;

    private final TimeProvider timeProvider;
    private final byte localIdentifier;
    private final Set<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();

    private Bytes identifierUpdatedBytes;
    private Bytes modDelBytes;

    private final ModificationDelegator modificationDelegator;
    private int startOfModificationIterators;

    public VanillaSharedReplicatedHashMap(@NotNull SharedHashMapBuilder builder,
                                          @NotNull Class<K> kClass,
                                          @NotNull Class<V> vClass) throws IOException {
        super(builder, kClass, vClass);

        this.timeProvider = builder.timeProvider();
        this.localIdentifier = builder.identifier();
        File file = builder.file();
        ObjectSerializer objectSerializer = builder.objectSerializer();
        BytesStore bytesStore = file == null
                ? DirectStore.allocateLazy(sizeInBytes(), objectSerializer)
                : new MappedStore(file, FileChannel.MapMode.READ_WRITE, sizeInBytes(), objectSerializer);
        createMappedStoreAndSegments(bytesStore);

        modificationDelegator = new ModificationDelegator(eventListener, modDelBytes, startOfModificationIterators);
        this.eventListener = modificationDelegator;
    }

    /**
     * this is used to iterate over all the modification iterators
     *
     * @return
     */
    int assignedModIterBitSetSizeInBytes() {
        return (int) align64((long) Math.ceil(127 + RESERVED_MOD_ITER / 8));
    }

    @Override
    VanillaSharedHashMap<K, V>.Segment createSegment(NativeBytes bytes, int index) {
        return new Segment(bytes, index);
    }

    Class segmentType() {
        return Segment.class;
    }

    int modIterBitSetSizeInBytes() {
        return (int) align64(bitsPerSegmentInModIterBitSet() * segments.length / 8);
    }

    private long bitsPerSegmentInModIterBitSet() {
        // min 128 * 8 to prevent false sharing on updating bits from different segments
        return Maths.nextPower2((long) entriesPerSegment, 128 * 8);
    }

    @Override
    int multiMapsPerSegment() {
        return 2;
    }

    int getHeaderSize() {
        final int headerSize = super.getHeaderSize();

        return headerSize + LAST_UPDATED_HEADER_SIZE + (modIterBitSetSizeInBytes() * (128 +
                RESERVED_MOD_ITER)) + assignedModIterBitSetSizeInBytes();
    }

    void setLastModificationTime(byte identifier, long timestamp) {
        final int offset = identifier * 8;

        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        if (identifierUpdatedBytes.readLong(offset) < timestamp)
            identifierUpdatedBytes.writeLong(offset, timestamp);
    }


    @Override
    public long lastModificationTime(byte remoteIdentifier) {
        assert remoteIdentifier != this.identifier();

        final int offset = remoteIdentifier * 8;
        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        return identifierUpdatedBytes.readLong(offset);
    }


    @Override
    void onHeaderCreated() {

        int offset = super.getHeaderSize();

        identifierUpdatedBytes = ms.bytes(offset, LAST_UPDATED_HEADER_SIZE).zeroOut();
        offset += LAST_UPDATED_HEADER_SIZE;

        modDelBytes = ms.bytes(offset, assignedModIterBitSetSizeInBytes()).zeroOut();
        offset += assignedModIterBitSetSizeInBytes();

        startOfModificationIterators = offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return put0(key, value, true, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * Used in conjunction with map replication, all put events that originate from a remote node will be
     * processed using this method.
     *
     * @param key        key with which the specified value is to be associated
     * @param value      value to be associated with the specified key
     * @param identifier a unique identifier for a replicating node
     * @param timeStamp  timestamp in milliseconds, when the put event originally occurred
     * @return the previous value
     * @see #put(Object, Object)
     */

    V put(K key, V value, byte identifier, long timeStamp) {
        assert identifier > 0;
        return put0(key, value, true, identifier, timeStamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V putIfAbsent(@net.openhft.lang.model.constraints.NotNull K key, V value) {
        return put0(key, value, false, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * @param key              key with which the specified value is associated
     * @param value            value expected to be associated with the specified key
     * @param replaceIfPresent set to false for putIfAbsent()
     * @param identifier       used to identify which replicating node made the change
     * @param timeStamp        the time that that change was made, this is used for replication
     * @return the value that was replaced
     */
    private V put0(K key, V value, boolean replaceIfPresent,
                   final byte identifier, long timeStamp) {
        checkKey(key);
        checkValue(value);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).put(keyBytes, key, value, segmentHash, replaceIfPresent,
                identifier, timeStamp);
    }

    /**
     * @param segmentNum a unique index of the segment
     * @return the segment associated with the {@code segmentNum}
     */
    private Segment segment(int segmentNum) {
        return (Segment) segments[segmentNum];
    }

    @Override
    V lookupUsing(K key, V value, boolean create) {
        checkKey(key);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).acquire(keyBytes, key, value, segmentHash, create,
                timeProvider.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(final Object key) {
        return removeIfValueIs(key, null, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * Used in conjunction with map replication, all remove events that originate from a remote node will be
     * processed using this method.
     *
     * @param key        key with which the specified value is associated
     * @param value      value expected to be associated with the specified key
     * @param identifier a unique identifier for a replicating node
     * @param timeStamp  timestamp in milliseconds, when the remove event originally occurred
     * @return {@code true} if the entry was removed
     * @see #remove(Object, Object)
     */

    V remove(K key, V value, byte identifier, long timeStamp) {
        assert identifier > 0;
        return removeIfValueIs(key, null, identifier, timeStamp);
    }


    void addCloseable(Closeable closeable) {
        closeables.add(closeable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
            }
        }

        try {
            // give time for stuff to fully close
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.close();

    }


    /**
     * {@inheritDoc}
     */
    @Override
    public byte identifier() {
        return localIdentifier;
    }

    @Override
    public Replica.ModificationIterator acquireModificationIterator
            (short remoteIdentifier,
             @NotNull final ModificationNotifier modificationNotifier) {

        return modificationDelegator.acquireModificationIterator(remoteIdentifier, modificationNotifier);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(@net.openhft.lang.model.constraints.NotNull final Object key, final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return removeIfValueIs(key, (V) value,
                localIdentifier, timeProvider.currentTimeMillis()) != null;
    }

    private V removeIfValueIs(final Object key, final V expectedValue,
                              final byte identifier, final long timestamp) {
        checkKey(key);
        Bytes keyBytes = getKeyAsBytes((K) key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).remove(keyBytes, (K) key, expectedValue, segmentHash,
                timestamp, identifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    V replaceIfValueIs(@net.openhft.lang.model.constraints.NotNull final K key, final V existingValue, final V newValue) {
        checkKey(key);
        checkValue(newValue);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).replace(keyBytes, key, existingValue, newValue, segmentHash,
                timeProvider.currentTimeMillis());
    }


    class Segment extends VanillaSharedHashMap<K, V>.Segment {

        private volatile IntIntMultiMap hashLookupLiveAndDeleted;
        private volatile IntIntMultiMap hashLookupLiveOnly;

        Segment(NativeBytes bytes, int index) {
            super(bytes, index);
        }

        @Override
        void createHashLookups(long start) {
            hashLookupLiveAndDeleted = createMultiMap(start);

            start += sizeOfMultiMap();
            hashLookupLiveOnly = createMultiMap(start);

        }




        private long entrySize(long keyLen, long valueLen) {
            long result = alignment.alignAddr(metaDataBytes +
                    expectedStopBits(keyLen) + keyLen + 10 +
                    expectedStopBits(valueLen)) + valueLen;
            // replication enforces that the entry size will never be larger than an unsigned short
            if (result > MAX_UNSIGNED_SHORT)
                throw new IllegalStateException("ENTRY WRITE_BUFFER_SIZE TOO LARGE : Replicated " +
                        "SharedHashMap's" +
                        " are restricted to an " +
                        "entry size of " + MAX_UNSIGNED_SHORT + ", " +
                        "your entry size=" + result);
            return result;
        }


        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#acquire(net.openhft.lang.io.Bytes,
         * Object, Object, int, boolean)
         */
        V acquire(Bytes keyBytes, K key, V usingValue, int hash2, boolean create, long timestamp) {
            lock();
            try {
                MultiStoreBytes entry = tmpBytes;
                long offset = searchKey(keyBytes, hash2, entry, hashLookupLiveOnly);
                if (offset >= 0) {

                    // skip the timestamp, identifier and is deleted flag
                    entry.skip(10);

                    return onKeyPresentOnAcquire(key, usingValue, offset, entry);
                } else {
                    usingValue = tryObtainUsingValueOnAcquire(keyBytes, key, usingValue, create);
                    if (usingValue != null) {
                        // see VanillaSharedHashMap.Segment.acquire() for explanation
                        // why `usingValue` is `create`.
                        offset = putEntryOnAcquire(keyBytes, hash2, usingValue, create, timestamp);
                        incrementSize();
                        notifyPut(offset, true, key, usingValue, posFromOffset(offset));
                        return usingValue;
                    } else {
                        return null;
                    }
                }
            } finally {
                unlock();
            }
        }

        private long putEntryOnAcquire(Bytes keyBytes, int hash2, V value, boolean usingValue,
                                       long timestamp) {
            return putEntry(keyBytes, hash2, value, usingValue, localIdentifier,
                    timestamp, hashLookupLiveOnly);
        }


        /**
         * called from a remote node as part of replication
         */
        private void remoteRemove(Bytes keyBytes, int hash2,
                                  final long timestamp, final byte identifier) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;

                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = entry.position();

                    if (shouldIgnore(entry, timestamp, identifier)) {
                        return;
                    }

                    // skip the is deleted flag
                    boolean wasDeleted = entry.readBoolean();

                    if (!wasDeleted) {
                        hashLookupLiveOnly.remove(hash2, pos);
                        decrementSize();
                    }

                    entry.position(timeStampPos);
                    entry.writeLong(timestamp);
                    assert identifier > 0;
                    entry.writeByte(identifier);
                    // was deleted
                    entry.writeBoolean(true);
                }
                // key is not found
                if (LOG.isDebugEnabled())
                    LOG.debug("Segment.remoteRemove() : key=" + keyBytes.toString().trim() + " was not " +
                            "found");
            } finally {
                unlock();
            }
        }


        /**
         * called from a remote node when it wishes to propagate a remove event
         */
        private void remotePut(@NotNull final Bytes inBytes, int hash2,
                               final byte identifier, final long timestamp,
                               long valuePos, long valueLimit) {
            lock();
            try {
                // inBytes position and limit correspond to the key
                final long keyLen = inBytes.remaining();

                hashLookupLiveAndDeleted.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0; ) {

                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(inBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    final long timeStampPos = entry.positionAddr();

                    if (shouldIgnore(entry, timestamp, identifier)) {
                        return;
                    }

                    boolean wasDeleted = entry.readBoolean();
                    entry.positionAddr(timeStampPos);
                    entry.writeLong(timestamp);
                    assert identifier > 0;
                    entry.writeByte(identifier);

                    // deleted flag
                    entry.writeBoolean(false);

                    long valueLenPos = entry.position();
                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;

                    // write the value
                    inBytes.limit(valueLimit);
                    inBytes.position(valuePos);

                    putValue(pos, offset, entry, valueLenPos, entryEndAddr, inBytes, null, true,
                            hashLookupLiveAndDeleted);

                    if (wasDeleted) {
                        // remove() would have got rid of this so we have to add it back in
                        hashLookupLiveOnly.put(hash2, pos);
                        incrementSize();
                    }
                    return;
                }

                // key is not found
                long valueLen = valueLimit - valuePos;
                int pos = alloc(inBlocks(entrySize(keyLen, valueLen)));
                long offset = offsetFromPos(pos);
                clearMetaData(offset);
                NativeBytes entry = entry(offset);

                entry.writeStopBit(keyLen);

                // write the key
                entry.write(inBytes);

                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                entry.writeStopBit(valueLen);
                alignment.alignPositionAddr(entry);

                // write the value
                inBytes.limit(valueLimit);
                inBytes.position(valuePos);

                entry.write(inBytes);

                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);

                incrementSize();

            } finally {
                unlock();
            }
        }

        V put(Bytes keyBytes, K key, V value, int hash2, boolean replaceIfPresent,
              final byte identifier, final long timestamp) {
            lock();
            try {
                IntIntMultiMap hashLookup = hashLookupLiveAndDeleted;
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                int pos;
                while ((pos = hashLookup.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    final long timeStampPos = entry.positionAddr();

                    if (shouldIgnore(entry, timestamp, identifier))
                        return null;

                    boolean wasDeleted = entry.readBoolean();

                    // if wasDeleted==true then we even if replaceIfPresent==false we can treat it
                    // the same way as replaceIfPresent true
                    if (replaceIfPresent || wasDeleted) {

                        entry.positionAddr(timeStampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // deleted flag
                        entry.writeBoolean(false);

                        final V prevValue = replaceValueOnPut(key, value, entry, pos, offset,
                                !wasDeleted && !putReturnsNull, hashLookup);

                        if (wasDeleted) {
                            // remove() would have got rid of this so we have to add it back in
                            hashLookupLiveOnly.put(hash2, pos);
                            incrementSize();
                            return null;
                        } else {
                            return prevValue;
                        }
                    } else {
                        // we wont replaceIfPresent and the entry is not deleted
                        return putReturnsNull ? null : readValue(entry, null);
                    }
                }

                // key is not found
                long offset = putEntry(keyBytes, hash2, value, false, identifier, timestamp, hashLookup);
                incrementSize();
                notifyPut(offset, true, key, value, posFromOffset(offset));
                return null;
            } finally {
                unlock();
            }
        }

        /**
         * Used only with replication, its sometimes possible to receive an old ( or stale update ) from a
         * remote map. This method is used to determine if we should ignore such updates.
         *
         * <p>We can reject put() and removes() when comparing times stamps with remote systems
         *
         * @param entry      the maps entry
         * @param timestamp  the time the entry was created or updated
         * @param identifier the unique identifier relating to this map
         * @return true if the entry should not be processed
         */
        private boolean shouldIgnore(@NotNull final NativeBytes entry, final long timestamp, final byte identifier) {

            final long lastModifiedTimeStamp = entry.readLong();

            // if the readTimeStamp is newer then we'll reject this put()
            // or they are the same and have a larger id
            if (lastModifiedTimeStamp < timestamp) {
                entry.skip(1); // skip the byte used for the identifier
                return false;
            }

            if (lastModifiedTimeStamp > timestamp)
                return true;

            // check the identifier
            return entry.readByte() > identifier;

        }

        /**
         * Puts entry. If {@code value} implements {@link net.openhft.lang.model.Byteable} interface and
         * {@code usingValue} is {@code true}, the value is backed with the bytes of this entry.
         *
         * @param keyBytes           serialized key
         * @param hash2              a hash of the {@code keyBytes}. Caller was searching for the key in the
         *                           {@code searchedHashLookup} using this hash.
         * @param value              the value to put
         * @param usingValue         {@code true} if the value should be backed with the bytes of the entry,
         *                           if it implements {@link net.openhft.lang.model.Byteable} interface,
         *                           {@code false} if it should put itself
         * @param identifier         the identifier of the outer SHM node
         * @param timestamp          the timestamp when the entry was put <s>(this could be later if it was a
         *                           remote put)</s> this method is called only from usual put or acquire
         * @param searchedHashLookup the hash lookup that used to find the entry based on the key
         * @return offset of the written entry in the Segment bytes
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#putEntry(net.openhft.lang.io.Bytes,
         * Object, boolean)
         */
        private long putEntry(Bytes keyBytes, int hash2, V value, boolean usingValue,
                              final int identifier, final long timestamp,
                              IntIntMultiMap searchedHashLookup) {
            long keyLen = keyBytes.remaining();

            // "if-else polymorphism" is not very beautiful, but allows to
            // reuse the rest code of this method and doesn't hurt performance.
            boolean byteableValue = usingValue && value instanceof Byteable;
            long valueLen;
            Bytes valueBytes = null;
            Byteable valueAsByteable = null;
            if (!byteableValue) {
                valueBytes = getValueAsBytes(value);
                valueLen = valueBytes.remaining();
            } else {
                valueAsByteable = (Byteable) value;
                valueLen = valueAsByteable.maxSize();
            }

            long entrySize = entrySize(keyLen, valueLen);
            int pos = alloc(inBlocks(entrySize));
            long offset = offsetFromPos(pos);
            clearMetaData(offset);
            NativeBytes entry = entry(offset);

            entry.writeStopBit(keyLen);
            entry.write(keyBytes);
            entry.writeLong(timestamp);
            entry.writeByte(identifier);
            entry.writeBoolean(false);

            writeValueOnPutEntry(valueLen, valueBytes, valueAsByteable, entry);

            // we have to add it both to the live
            if (searchedHashLookup == hashLookupLiveAndDeleted) {
                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);
            } else {
                hashLookupLiveOnly.putAfterFailedSearch(pos);
                hashLookupLiveAndDeleted.put(hash2, pos);
            }

            return offset;
        }

        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#remove(net.openhft.lang.io.Bytes, Object,
         * Object, int)
         */
        public V remove(Bytes keyBytes, K key, V expectedValue, int hash2,
                        final long timestamp, final byte identifier) {
            assert identifier > 0;
            lock();
            try {
                long keyLen = keyBytes.remaining();

                final IntIntMultiMap multiMap = hashLookupLiveAndDeleted;

                multiMap.startSearch(hash2);
                for (int pos; (pos = multiMap.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = entry.position();
                    if (shouldIgnore(entry, timestamp, identifier)) {
                        return null;
                    }
                    // skip the is deleted flag
                    final boolean wasDeleted = entry.readBoolean();
                    if (wasDeleted) {
                        // this caters for the case when the entry in not in our hashLookupLiveOnly
                        // map but maybe in our hashLookupLiveAndDeleted,
                        // so we have to send the deleted notification
                        entry.position(timeStampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // was deleted is already true
                        entry.skip(1);

                        notifyRemoved(offset, key, null, pos);
                        return null;
                    }

                    long valueLen = readValueLen(entry);
                    V valueRemoved = expectedValue != null || !removeReturnsNull
                            ? readValue(entry, null, valueLen) : null;

                    if (expectedValue != null && !expectedValue.equals(valueRemoved))
                        return null;

                    hashLookupLiveOnly.remove(hash2, pos);
                    decrementSize();

                    entry.position(timeStampPos);
                    entry.writeLong(timestamp);
                    entry.writeByte(identifier);
                    entry.writeBoolean(true);

                    notifyRemoved(offset, key, valueRemoved, pos);
                    return valueRemoved;
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }

        @Override
        IntIntMultiMap containsKeyHashLookup() {
            return hashLookupLiveOnly;
        }

        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#remove(net.openhft.lang.io.Bytes, Object,
         * Object, int)
         */
        public V replace(Bytes keyBytes, K key, V expectedValue, V newValue, int hash2,
                         long timestamp) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveOnly.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveOnly.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    if (shouldIgnore(entry, timestamp, localIdentifier))
                        return null;
                    // skip the is deleted flag
                    entry.skip(1);

                    return onKeyPresentOnReplace(key, expectedValue, newValue, pos, offset, entry,
                            hashLookupLiveOnly);
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }

        @Override
        void replacePosInHashLookupOnRelocation(IntIntMultiMap searchedHashLookup, int prevPos, int pos) {
            searchedHashLookup.replacePrevPos(pos);
            int hash = searchedHashLookup.getSearchHash();
            IntIntMultiMap anotherLookup = searchedHashLookup == hashLookupLiveAndDeleted ?
                    hashLookupLiveOnly : hashLookupLiveAndDeleted;
            anotherLookup.replace(hash, prevPos, pos);
        }

        public void dirtyEntries(final long timeStamp,
                                 final ModificationIterator.EntryModifiableCallback callback) {

            this.lock();
            try {
                final int index = Segment.this.getIndex();
                hashLookupLiveAndDeleted.forEach(new IntIntMultiMap.EntryConsumer() {

                    @Override
                    public void accept(int hash, int pos) {
                        final NativeBytes entry = entry(offsetFromPos(pos));
                        long keyLen = entry.readStopBit();
                        entry.skip(keyLen);

                        final long entryTimestamp = entry.readLong();

                        if (entryTimestamp >= timeStamp &&
                                entry.readByte() == VanillaSharedReplicatedHashMap.this.identifier())
                            callback.set(index, pos);
                    }
                });

            } finally {
                unlock();
            }
        }


        /**
         * removes all the entries
         */
        void clear() {

            // we have to make sure that every calls notifies on remove,
            // so that the replicators can pick it up
            for (K k : keySet()) {
                VanillaSharedReplicatedHashMap.this.remove(k);
            }

        }

        void visit(IntIntMultiMap.EntryConsumer entryConsumer) {
            hashLookupLiveOnly.forEach(entryConsumer);
        }

        /**
         * returns a null value if the entry has been deleted
         *
         * @param pos
         * @return a null value if the entry has been deleted
         */
        @Nullable
        public Entry<K, V> getEntry(long pos) {
            long offset = offsetFromPos(pos);
            NativeBytes entry = entry(offset);
            entry.readStopBit();
            K key = entry.readInstance(kClass, null); //todo: readUsing?

            // skip timestamp and id
            entry.skip(10);

            V value = readValue(entry, null); //todo: reusable container
            return new WriteThroughEntry(key, value);
        }
    }


    /**
     * {@inheritDoc}
     *
     * <p>This method does not set a segment lock, A segment lock should be obtained before calling this
     * method, especially when being used in a multi threaded context.
     */
    @Override
    public void writeExternalEntry(@NotNull AbstractBytes entry, @NotNull Bytes destination, int chronicleId) {

        final long initialLimit = entry.limit();

        final long keyLen = entry.readStopBit();
        final long keyPosition = entry.position();
        entry.skip(keyLen);
        final long keyLimit = entry.position();
        final long timeStamp = entry.readLong();

        final byte identifier = entry.readByte();
        if (identifier != localIdentifier) {
            // although unlikely, this may occur if the entry has been updated
            return;
        }

        final boolean isDeleted = entry.readBoolean();
        long valueLen;
        if (!isDeleted) {
            valueLen = entry.readStopBit();
            assert valueLen > 0;
        } else {
            valueLen = 0;
        }

        final long valuePosition = entry.position();

        destination.writeStopBit(keyLen);
        destination.writeStopBit(valueLen);
        destination.writeStopBit(timeStamp);

        // we store the isDeleted flag in the identifier
        // ( when the identifier is negative it is deleted )
        destination.writeByte(isDeleted ? -identifier : identifier);

        // write the key
        entry.position(keyPosition);
        entry.limit(keyLimit);
        destination.write(entry);

        boolean debugEnabled = LOG.isDebugEnabled();
        String message = null;
        if (debugEnabled) {
            if (isDeleted) {
                LOG.debug("WRITING ENTRY TO DEST -  into local-id={}, remove(key={})",
                        localIdentifier, entry.toString().trim());
            } else {
                message = String.format(
                        "WRITING ENTRY TO DEST  -  into local-id=%d, put(key=%s,",
                        localIdentifier, entry.toString().trim());
            }
        }

        if (isDeleted)
            return;

        entry.limit(initialLimit);
        entry.position(valuePosition);
        // skipping the alignment, as alignment wont work when we send the data over the wire.
        alignment.alignPositionAddr(entry);

        // writes the value
        entry.limit(entry.position() + valueLen);
        destination.write(entry);

        if (debugEnabled) {
            LOG.debug(message + "value=" + entry.toString().trim() + ")");
        }
    }


    /**
     * {@inheritDoc}
     *
     * <p>This method does not set a segment lock, A segment lock should be obtained before calling this
     * method, especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(@NotNull Bytes source) {

        final long keyLen = source.readStopBit();
        final long valueLen = source.readStopBit();
        final long timeStamp = source.readStopBit();
        final byte id = source.readByte();
        final byte remoteIdentifier;
        final boolean isDeleted;

        if (id < 0) {
            isDeleted = true;
            remoteIdentifier = (byte) -id;
        } else if (id != 0) {
            isDeleted = false;
            remoteIdentifier = id;
        } else {
            throw new IllegalStateException("identifier can't be 0");
        }

        if (remoteIdentifier == VanillaSharedReplicatedHashMap.this.identifier()) {
            // this may occur when working with UDP, as we will receive our own data
            return;
        }

        final long keyPosition = source.position();
        final long keyLimit = keyPosition + keyLen;

        source.limit(keyLimit);
        long hash = Hasher.hash(source);

        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);

        boolean debugEnabled = LOG.isDebugEnabled();

        if (isDeleted) {

            if (debugEnabled) {
                LOG.debug(
                        "READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})",
                        localIdentifier, remoteIdentifier, source.toString().trim()
                );
            }

            segment(segmentNum).remoteRemove(source, segmentHash, timeStamp, remoteIdentifier);
            setLastModificationTime(remoteIdentifier, timeStamp);
            return;
        }

        String message = null;
        if (debugEnabled) {
            message = String.format(
                    "READING FROM SOURCE -  into local-id=%d, remote-id=%d, put(key=%s,",
                    localIdentifier, remoteIdentifier, source.toString().trim());
        }

        final long valuePosition = keyLimit;
        final long valueLimit = valuePosition + valueLen;
        segment(segmentNum).remotePut(source, segmentHash, remoteIdentifier, timeStamp,
                valuePosition, valueLimit);
        setLastModificationTime(remoteIdentifier, timeStamp);

        if (debugEnabled) {
            source.limit(valueLimit);
            source.position(valuePosition);
            LOG.debug(message + "value=" + source.toString().trim() + ")");
        }
    }


    /**
     * receive an update from the map, via the SharedMapEventListener and delegates the changes to the
     * currently active modification iterators
     */
    class ModificationDelegator extends SharedMapEventListener<K, V, SharedHashMap<K, V>> {

        // the assigned modification iterators
        private final ATSDirectBitSet bitSet;

        private final AtomicReferenceArray<ModificationIterator> modificationIterators =
                new AtomicReferenceArray<ModificationIterator>(127 + RESERVED_MOD_ITER);

        private final SharedMapEventListener<K, V, SharedHashMap<K, V>> nextListener;
        private long startOfModificationIterators;

        public ModificationDelegator(@NotNull final SharedMapEventListener<K, V, SharedHashMap<K, V>> nextListener,
                                     final Bytes bytes, long startOfModificationIterators) {
            this.nextListener = nextListener;
            this.startOfModificationIterators = startOfModificationIterators;
            bitSet = new ATSDirectBitSet(bytes);

        }

        /**
         * {@inheritDoc}
         */
        public ModificationIterator acquireModificationIterator(
                final short remoteIdentifier,
                @NotNull final ModificationNotifier modificationNotifier) {

            final ModificationIterator modificationIterator = modificationIterators.get(remoteIdentifier);

            if (modificationIterator != null)
                return modificationIterator;

            synchronized (modificationIterators) {
                final ModificationIterator modificationIterator0 = modificationIterators.get(remoteIdentifier);

                if (modificationIterator0 != null)
                    return modificationIterator0;

                final Bytes bytes = ms.bytes(startOfModificationIterators + (modIterBitSetSizeInBytes()
                                * remoteIdentifier),
                        modIterBitSetSizeInBytes());

                final ModificationIterator newModificationIterator = new ModificationIterator(
                        bytes, modificationNotifier);

                modificationIterators.set(remoteIdentifier, newModificationIterator);
                bitSet.set(remoteIdentifier);
                return newModificationIterator;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPut(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                          boolean added, K key, V value, long pos, SharedSegment segment) {

            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onPut() is called from outside of the parent map";
            try {
                nextListener.onPut(map, entry, metaDataBytes, added, key, value, pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }

            for (long next = bitSet.nextSetBit(0); next > 0; next = bitSet.nextSetBit(next + 1)) {
                try {
                    final ModificationIterator modificationIterator = modificationIterators.get((int) next);
                    modificationIterator.onPut(map, entry, metaDataBytes,
                            added, key, value, pos, segment);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }


        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onRemove(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, int pos, SharedSegment segment) {
            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";
            try {
                nextListener.onRemove(map, entry, metaDataBytes, key, value, pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }

            for (long next = bitSet.nextSetBit(0); next > 0; next = bitSet.nextSetBit(next + 1)) {
                try {
                    final ModificationIterator modificationIterator = modificationIterators.get((int) next);
                    modificationIterator.onRemove(map, entry, metaDataBytes, key, value, pos, segment);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }

        }


        @Override
        public V onGetMissing(SharedHashMap<K, V> map, Bytes keyBytes, K key,
                              V usingValue) {
            return nextListener.onGetMissing(map, keyBytes, key, usingValue);
        }

        @Override
        public void onGetFound(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                               K key, V value) {
            nextListener.onGetFound(map, entry, metaDataBytes, key, value);
        }

    }

    /**
     * Once a change occurs to a map, map replication requires that these changes are picked up by another
     * thread, this class provides an iterator like interface to poll for such changes.
     *
     * <p>In most cases the thread that adds data to the node is unlikely to be the same thread that
     * replicates the data over to the other nodes, so data will have to be marshaled between the main thread
     * storing data to the map, and the thread running the replication.
     *
     * <p>One way to perform this marshalling, would be to pipe the data into a queue. However, This class
     * takes another approach. It uses a bit set, and marks bits which correspond to the indexes of the
     * entries that have changed. It then provides an iterator like interface to poll for such changes.
     *
     * @author Rob Austin.
     */
    class ModificationIterator extends SharedMapEventListener<K, V, SharedHashMap<K, V>>
            implements Replica.ModificationIterator {


        private final ModificationNotifier modificationNotifier;
        private final ATSDirectBitSet changes;
        private final int segmentIndexShift;
        private final long posMask;

        private final EntryModifiableCallback entryModifiableCallback = new EntryModifiableCallback();

        // records the current position of the cursor in the bitset
        private long position = -1;

        /**
         * @param bytes                the back the bitset, used to mark which entries have changed
         * @param modificationNotifier called when ever there is a change applied
         */
        public ModificationIterator(@NotNull final Bytes bytes,
                                    @NotNull final ModificationNotifier modificationNotifier) {

            this.modificationNotifier = modificationNotifier;
            long bitsPerSegment = bitsPerSegmentInModIterBitSet();
            segmentIndexShift = Long.numberOfTrailingZeros(bitsPerSegment);
            posMask = bitsPerSegment - 1;
            changes = new ATSDirectBitSet(bytes);
        }

        /**
         * used to merge multiple segments and positions into a single index used by the bit map
         *
         * @param segmentIndex the index of the maps segment
         * @param pos          the position within this {@code segmentIndex}
         * @return and index the has combined the {@code segmentIndex}  and  {@code pos} into a single value
         */
        private long combine(int segmentIndex, long pos) {
            return (((long) segmentIndex) << segmentIndexShift) | pos;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPut(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                          boolean added, K key, V value, long pos, SharedSegment segment) {

            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onPut() is called from outside of the parent map";


            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onRemove(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, int pos, SharedSegment segment) {
            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";


            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();

        }

        /**
         * Ensures that garbage in the old entry's location won't be broadcast as changed entry.
         */
        @Override
        void onRelocation(int pos, SharedSegment segment) {
            changes.clear(combine(segment.getIndex(), pos));
            // don't call nextListener.onRelocation(),
            // because no one event listener else overrides this method.
        }


        /**
         * you can continue to poll hasNext() until data becomes available. If are are in the middle of
         * processing an entry via {@code nextEntry}, hasNext will return true until the bit is cleared
         *
         * @return true if there is an entry
         */
        public boolean hasNext() {
            final long position = this.position;
            return changes.nextSetBit(position == NOT_FOUND ? 0 : position) != NOT_FOUND ||
                    (position > 0 && changes.nextSetBit(0) != NOT_FOUND);
        }


        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @param chronicleId
         * @return true if an entry was processed
         */
        public boolean nextEntry(@NotNull final AbstractEntryCallback entryCallback, final int chronicleId) {
            long position = this.position;
            while (true) {
                long oldPosition = position;
                position = changes.nextSetBit(oldPosition + 1);

                if (position == NOT_FOUND) {
                    if (oldPosition == NOT_FOUND) {
                        this.position = NOT_FOUND;
                        return false;
                    }
                    continue;
                }

                this.position = position;
                final SharedSegment segment = segment((int) (position >>> segmentIndexShift));
                segment.lock();
                try {
                    if (changes.clearIfSet(position)) {

                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        final NativeBytes entry = segment.entry(segment.offsetFromPos(segmentPos));

                        // if the entry should be ignored, we'll move the next entry
                        final boolean success = entryCallback.onEntry(entry, chronicleId);
                        entryCallback.onAfterEntry();
                        if (success) {
                            return true;
                        }
                    }

                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in onReplication()),
                    // go to pick up next (next iteration in while (true) loop)
                } finally {
                    segment.unlock();
                }
            }
        }

        /**
         * details about when a modification to an entry was made
         */
        class EntryModifiableCallback {

            /**
             * set the bit related to {@code segment} and {@code pos}
             *
             * @param segmentIndex the segment relating to the bit to set
             * @param pos          the position relating to the bit to set
             */
            public void set(int segmentIndex, int pos) {
                final long combine = combine(segmentIndex, pos);
                changes.set(combine);
            }
        }

        @Override
        public void dirtyEntries(long fromTimeStamp) {

            // iterate over all the segments and mark bit in the modification iterator
            // that correspond to entries with an older timestamp
            for (final Segment segment : (Segment[]) segments) {
                segment.dirtyEntries(fromTimeStamp, entryModifiableCallback);
            }

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K key(@NotNull AbstractBytes entry, K usingKey) {

        final long start = entry.position();
        try {

            // keyLen
            entry.readStopBit();

            final long keyPosition = entry.position();

            if (generatedValueType)
                if (usingKey == null)
                    usingKey = DataValueClasses.newDirectReference(kClass);
                else
                    assert usingKey instanceof Byteable;
            if (usingKey instanceof Byteable) {
                ((Byteable) usingKey).bytes(entry, keyPosition);
                return usingKey;
            }

            return entry.readInstance(kClass, usingKey);
        } finally {
            entry.position(start);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V value(@NotNull AbstractBytes entry, V usingValue) {
        final long start = entry.position();
        try {

            // keyLen
            entry.skip(entry.readStopBit());

            //timeStamp
            entry.readLong();

            final byte identifier = entry.readByte();
            if (identifier != localIdentifier) {
                return null;
            }

            final boolean isDeleted = entry.readBoolean();
            long valueLen;
            if (!isDeleted) {
                valueLen = entry.readStopBit();
                assert valueLen > 0;
            } else {
                return null;
            }

            final long valueOffset = entry.position();

            if (generatedValueType)
                if (usingValue == null)
                    usingValue = DataValueClasses.newDirectReference(vClass);
                else
                    assert usingValue instanceof Byteable;
            if (usingValue instanceof Byteable) {
                ((Byteable) usingValue).bytes(entry, valueOffset);
                return usingValue;
            }

            return entry.readInstance(vClass, usingValue);
        } finally {
            entry.position(start);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean wasRemoved(@NotNull AbstractBytes entry) {
        final long start = entry.position();
        try {
            return entry.readBoolean(entry.readStopBit() + 10);
        } finally {
            entry.position(start);
        }
    }

}


