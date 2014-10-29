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


import net.openhft.chronicle.hash.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.MetaBytesWriter;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.openhft.chronicle.hash.serialization.Hasher.hash;
import static net.openhft.lang.MemoryUnit.BITS;
import static net.openhft.lang.MemoryUnit.BYTES;
import static net.openhft.lang.MemoryUnit.CACHE_LINES;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;

/**
 * <h2>A Replicating Multi Master HashMap</h2>
 * <p>Each remote hash map, mirrors its changes over to
 * another remote hash map, neither hash map is considered the master store of data, each hash map
 * uses timestamps to reconcile changes. We refer to an instance of a remote hash-map as a node. A
 * node will be connected to any number of other nodes, for the first implementation the maximum
 * number of nodes will be fixed. The data that is stored locally in each node will become
 * eventually consistent. So changes made to one node, for example by calling put() will be
 * replicated over to the other node. To achieve a high level of performance and throughput, the
 * call to put() won’t block, with concurrentHashMap, It is typical to check the return code of some
 * methods to obtain the old value for example remove(). Due to the loose coupling and lock free
 * nature of this multi master implementation,  this return value will only be the old value on the
 * nodes local data store. In other words the nodes are only concurrent locally. Its worth realising
 * that another node performing exactly the same operation may return a different value. However
 * reconciliation will ensure the maps themselves become eventually consistent.
 * </p>
 * <h2>Reconciliation </h2>
 * <p>If two ( or more nodes ) were to receive a change to their maps for the
 * same key but different values, say by a user of the maps, calling the put(key, value). Then,
 * initially each node will update its local store and each local store will hold a different value,
 * but the aim of multi master replication is to provide eventual consistency across the nodes. So,
 * with multi master when ever a node is changed it will notify the other nodes of its change. We
 * will refer to this notification as an event. The event will hold a timestamp indicating the time
 * the change occurred, it will also hold the state transition, in this case it was a put with a key
 * and value. Eventual consistency is achieved by looking at the timestamp from the remote node, if
 * for a given key, the remote nodes timestamp is newer than the local nodes timestamp, then the
 * event from the remote node will be applied to the local node, otherwise the event will be
 * ignored. </p>
 * <p>However there is an edge case that we have to concern ourselves with, If two
 * nodes update their map at the same time with different values, we have to deterministically
 * resolve which update wins, because of eventual consistency both nodes should end up locally
 * holding the same data. Although it is rare two remote nodes could receive an update to their maps
 * at exactly the same time for the same key, we have to handle this edge case, its therefore
 * important not to rely on timestamps alone to reconcile the updates. Typically the update with the
 * newest timestamp should win, but in this example both timestamps are the same, and the decision
 * made to one node should be identical to the decision made to the other. We resolve this simple
 * dilemma by using a node identifier, each node will have a unique identifier, the update from the
 * node with the smallest identifier wins.
 * </p>
 *
 * @param <K> the entries key type
 * @param <V> the entries value type
 */
class ReplicatedChronicleMap<K, KI, MKI extends MetaBytesInterop<K, KI>,
        V, VW, MVW extends MetaBytesWriter<V, VW>>
        extends VanillaChronicleMap<K, KI, MKI, V, VW, MVW>
        implements ChronicleMap<K, V>,
        Replica, Replica.EntryExternalizable, Replica.EntryResolver<K, V>, Closeable {
    // for file, jdbc and UDP replication
    public static final int RESERVED_MOD_ITER = 8;
    static final int MAX_UNSIGNED_SHORT = Character.MAX_VALUE;
    private static final long serialVersionUID = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleMap.class);
    private static final long LAST_UPDATED_HEADER_SIZE = 128L * 8L;
    public static final int ADDITIONAL_ENTRY_BYTES = 10;
    private final TimeProvider timeProvider;
    private final byte localIdentifier;
    transient Set<Closeable> closeables;
    private transient Bytes identifierUpdatedBytes;

    private transient ModificationDelegator modificationDelegator;
    private transient long startOfModificationIterators;

    public ReplicatedChronicleMap(@NotNull AbstractChronicleMapBuilder<K, V, ?> builder)
            throws IOException {
        super(builder);
        this.timeProvider = builder.timeProvider();

        this.localIdentifier = builder.identifier();

        if (localIdentifier == -1) {
            throw new IllegalStateException("localIdentifier should not be -1");
        }

    }

    private int assignedModIterBitSetSizeInBytes() {
        return (int) CACHE_LINES.align(BYTES.alignAndConvert(127 + RESERVED_MOD_ITER, BITS), BYTES);
    }

    @Override
    Segment createSegment(NativeBytes bytes, int index) {
        return new Segment(bytes, index);
    }

    Class segmentType() {
        return Segment.class;
    }

    @Override
    void initTransients() {
        super.initTransients();
        closeables = new CopyOnWriteArraySet<Closeable>();
    }

    long modIterBitSetSizeInBytes() {
        long bytes = BITS.toBytes(bitsPerSegmentInModIterBitSet() * segments.length);
        return CACHE_LINES.align(bytes, BYTES);
    }

    private long bitsPerSegmentInModIterBitSet() {
        // min 128 * 8 to prevent false sharing on updating bits from different segments
        // TODO this doesn't prevent false sharing. There should be GAPS between per-segment bits
        return Maths.nextPower2(entriesPerSegment, 128L * 8L);
    }

    @Override
    int multiMapsPerSegment() {
        return 2;
    }

    @Override
    long getHeaderSize() {
        return super.getHeaderSize() + LAST_UPDATED_HEADER_SIZE +
                (modIterBitSetSizeInBytes() * (128 + RESERVED_MOD_ITER)) +
                assignedModIterBitSetSizeInBytes();
    }

    void setLastModificationTime(byte identifier, long timestamp) {
        final long offset = identifier * 8L;

        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        if (identifierUpdatedBytes.readLong(offset) < timestamp)
            identifierUpdatedBytes.writeLong(offset, timestamp);
    }


    @Override
    public long lastModificationTime(byte remoteIdentifier) {
        assert remoteIdentifier != this.identifier();

        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        return identifierUpdatedBytes.readLong(remoteIdentifier * 8L);
    }

    @Override
    void onHeaderCreated() {

        long offset = super.getHeaderSize();

        identifierUpdatedBytes = ms.bytes(offset, LAST_UPDATED_HEADER_SIZE).zeroOut();
        offset += LAST_UPDATED_HEADER_SIZE;

        Bytes modDelBytes = ms.bytes(offset, assignedModIterBitSetSizeInBytes()).zeroOut();
        offset += assignedModIterBitSetSizeInBytes();

        startOfModificationIterators = offset;

        modificationDelegator = new ModificationDelegator(eventListener, modDelBytes);
    }

    @Override
    MapEventListener<K, V, ChronicleMap<K, V>> eventListener() {
        return modificationDelegator;
    }

    @Override
    public V put(K key, V value) {
        return put0(key, value, true, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * Used in conjunction with map replication, all put events that originate from a remote node
     * will be processed using this method.
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
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyWriter =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);
        long hash = metaKeyWriter.hash(keyWriter, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segment(segmentNum).put(copies, metaKeyWriter, keyWriter, key, value, segmentHash,
                replaceIfPresent, identifier, timeStamp);
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
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyWriter =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);
        long hash = metaKeyWriter.hash(keyWriter, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segment(segmentNum).acquire(copies, metaKeyWriter, keyWriter, key, value,
                segmentHash, create, create ? timeProvider.currentTimeMillis() : 0L);
    }

    @Override
    public V remove(final Object key) {
        return removeIfValueIs(key, null, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * Used in conjunction with map replication, all remove events that originate from a remote node
     * will be processed using this method.
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
        return removeIfValueIs(key, value, identifier, timeStamp);
    }

    void addCloseable(Closeable closeable) {
        closeables.add(closeable);
    }

    @Override
    public void close() {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
            }
        }

        super.close();

    }

    @Override
    public byte identifier() {
        return localIdentifier;
    }

    @Override
    public Replica.ModificationIterator acquireModificationIterator
            (short remoteIdentifier,
             @NotNull final ModificationNotifier modificationNotifier) {

        return modificationDelegator
                .acquireModificationIterator(remoteIdentifier, modificationNotifier);
    }

    @Override
    public boolean remove(@NotNull final Object key, final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return removeIfValueIs(key, (V) value,
                localIdentifier, timeProvider.currentTimeMillis()) != null;
    }

    private V removeIfValueIs(final Object k, final V expectedValue,
                              final byte identifier, final long timestamp) {
        checkKey(k);
        K key = (K) k;
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyWriter =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);
        long hash = metaKeyWriter.hash(keyWriter, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segment(segmentNum).remove(copies, metaKeyWriter, keyWriter, key, expectedValue,
                segmentHash, timestamp, identifier);
    }

    @Override
    V replaceIfValueIs(@NotNull final K key, final V existingValue, final V newValue) {
        checkKey(key);
        checkValue(newValue);
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyWriter =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);
        long hash = metaKeyWriter.hash(keyWriter, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segment(segmentNum).replace(copies, metaKeyWriter, keyWriter, key, existingValue,
                newValue, segmentHash, timeProvider.currentTimeMillis());
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void writeExternalEntry(@NotNull Bytes entry, @NotNull Bytes destination,
                                   int chronicleId) {

        final long initialLimit = entry.limit();

        final long keySize = keySizeMarshaller.readSize(entry);
        final long keyPosition = entry.position();
        entry.skip(keySize);
        final long keyLimit = entry.position();
        final long timeStamp = entry.readLong();

        final byte identifier = entry.readByte();
        if (identifier != localIdentifier) {
            // although unlikely, this may occur if the entry has been updated
            return;
        }

        final boolean isDeleted = entry.readBoolean();
        long valueSize;
        if (!isDeleted) {
            valueSize = valueSizeMarshaller.readSize(entry);
        } else {
            valueSize = 0L;
        }

        final long valuePosition = entry.position();

        keySizeMarshaller.writeSize(destination, keySize);
        valueSizeMarshaller.writeSize(destination, valueSize);
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
        entry.limit(entry.position() + valueSize);
        destination.write(entry);

        if (debugEnabled) {
            LOG.debug(message + "value=" + entry.toString().trim() + ")");
        }
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(@NotNull Bytes source) {

        final long keySize = keySizeMarshaller.readSize(source);
        final long valueSize = valueSizeMarshaller.readSize(source);
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

        if (remoteIdentifier == ReplicatedChronicleMap.this.identifier()) {
            // this may occur when working with UDP, as we may receive our own data
            return;
        }

        final long keyPosition = source.position();
        final long keyLimit = keyPosition + keySize;

        source.limit(keyLimit);
        long hash = hash(source);

        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);

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
        final long valueLimit = valuePosition + valueSize;
        segment(segmentNum).remotePut(source, segmentHash, remoteIdentifier, timeStamp,
                valuePosition, valueLimit);
        setLastModificationTime(remoteIdentifier, timeStamp);

        if (debugEnabled) {
            source.limit(valueLimit);
            source.position(valuePosition);
            LOG.debug(message + "value=" + source.toString().trim() + ")");
        }
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public K key(@NotNull Bytes entry, K usingKey) {
        final long start = entry.position();
        try {
            long keySize = keySizeMarshaller.readSize(entry);
            ThreadLocalCopies copies = keyReaderProvider.getCopies(null);
            return keyReaderProvider.get(copies, originalKeyReader).read(entry, keySize);
        } finally {
            entry.position(start);
        }

    }

    @Override
    public V value(@NotNull Bytes entry, V usingValue) {
        final long start = entry.position();
        try {
            entry.skip(keySizeMarshaller.readSize(entry));

            //timeStamp
            entry.readLong();

            final byte identifier = entry.readByte();
            if (identifier != localIdentifier) {
                return null;
            }

            final boolean isDeleted = entry.readBoolean();
            long valueSize;
            if (!isDeleted) {
                valueSize = valueSizeMarshaller.readSize(entry);
                assert valueSize > 0;
            } else {
                return null;
            }
            alignment.alignPositionAddr(entry);
            ThreadLocalCopies copies = valueReaderProvider.getCopies(null);
            BytesReader<V> valueReader = valueReaderProvider.get(copies, originalValueReader);
            return valueReader.read(entry, valueSize, usingValue);
        } finally {
            entry.position(start);
        }
    }

    @Override
    public boolean wasRemoved(@NotNull Bytes entry) {
        final long start = entry.position();
        try {
            return entry.readBoolean(keySizeMarshaller.readSize(entry) + 9L);
        } finally {
            entry.position(start);
        }
    }

    class Segment extends VanillaChronicleMap<K, KI, MKI, V, VW, MVW>.Segment {

        private volatile MultiMap hashLookupLiveAndDeleted;
        private volatile MultiMap hashLookupLiveOnly;

        Segment(NativeBytes bytes, int index) {
            super(bytes, index);
        }

        @Override
        void createHashLookups(long start) {
            hashLookupLiveAndDeleted = createMultiMap(start);
            start += CACHE_LINES.align(sizeOfMultiMap() + sizeOfMultiMapBitSet(), BYTES);
            hashLookupLiveOnly = createMultiMap(start);
        }

        public MultiMap getHashLookup() {
            return hashLookupLiveOnly;
        }

        private long entrySize(long keySize, long valueSize) {
            long result = alignment.alignAddr(metaDataBytes +
                    keySizeMarshaller.sizeEncodingSize(keySize) + keySize + ADDITIONAL_ENTRY_BYTES +
                    valueSizeMarshaller.sizeEncodingSize(valueSize)) + valueSize;
            // replication enforces that the entry size will never be larger than an unsigned short
            if (result > MAX_UNSIGNED_SHORT)
                throw new IllegalStateException("ENTRY WRITE_BUFFER_SIZE TOO LARGE : Replicated " +
                        "ChronicleMap's" +
                        " are restricted to an " +
                        "entry size of " + MAX_UNSIGNED_SHORT + ", " +
                        "your entry size=" + result);
            return result;
        }


        /**
         * @see VanillaChronicleMap.Segment#acquire
         */
        V acquire(ThreadLocalCopies copies, MKI metaKeyInterop, KI keyInterop,
                  K key, V usingValue, long hash2, boolean create, long timestamp) {
            if (create)
                writeLock();
            else
                readLock();
            try {
                long keySize = metaKeyInterop.size(keyInterop, key);
                MultiStoreBytes entry = acquireTmpBytes();
                long offset = searchKey(keyInterop, metaKeyInterop, key, keySize, hash2, entry,
                        hashLookupLiveOnly);
                if (offset >= 0L) {
                    return onKeyPresentOnAcquire(copies, key, usingValue, offset, entry);
                } else {
                    if (!create)
                        return null;
                    if (defaultValueProvider != null) {
                        V defaultValue = defaultValueProvider.get(key);
                        copies = valueWriterProvider.getCopies(copies);
                        VW valueWriter = valueWriterProvider.get(copies, originalValueWriter);
                        copies = metaValueWriterProvider.getCopies(copies);
                        MetaBytesWriter<V, VW> metaValueWriter = metaValueWriterProvider.get(
                                copies, originalMetaValueWriter, valueWriter, defaultValue);

                        offset = putEntry(metaKeyInterop, keyInterop, key, keySize, hash2,
                                localIdentifier, timestamp, hashLookupLiveOnly,
                                metaValueWriter, valueWriter, defaultValue);
                    } else {
                        offset = putEntry(metaKeyInterop, keyInterop, key, keySize, hash2,
                                localIdentifier, timestamp, hashLookupLiveOnly,
                                prepareValueBytesAsWriter, null, key);
                    }
                    return onKeyAbsentOnAcquire(copies, key, keySize, usingValue, offset);
                }
            } finally {
                if (create)
                    writeUnlock();
                else
                    readUnlock();
            }
        }

        @Override
        void skipReplicationBytes(Bytes bytes) {
            bytes.skip(ADDITIONAL_ENTRY_BYTES);
        }

        /**
         * called from a remote node as part of replication
         */
        private void remoteRemove(Bytes keyBytes, long hash2,
                                  final long timestamp, final byte identifier) {
            writeLock();
            try {
                long keySize = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                for (long pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!remoteKeyEquals(keyBytes, keySize, entry))
                        continue;

                    // key is found
                    entry.skip(keySize);

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
                    LOG.debug("Segment.remoteRemove() : key=" + keyBytes.toString().trim() +
                            " was not found");
            } finally {
                writeUnlock();
            }
        }


        /**
         * called from a remote node when it wishes to propagate a remove event
         */
        private void remotePut(@NotNull final Bytes inBytes, long hash2,
                               final byte identifier, final long timestamp,
                               long valuePos, long valueLimit) {
            writeLock();
            try {
                // inBytes position and limit correspond to the key
                final long keySize = inBytes.remaining();

                hashLookupLiveAndDeleted.startSearch(hash2);
                for (long pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0L; ) {

                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!remoteKeyEquals(inBytes, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

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

                    long valueSizePos = entry.position();
                    long valueSize = readValueSize(entry);
                    long entryEndAddr = entry.positionAddr() + valueSize;

                    // write the value
                    inBytes.limit(valueLimit);
                    inBytes.position(valuePos);

                    putValue(pos, offset, entry, valueSizePos, entryEndAddr, null, null, inBytes,
                            hashLookupLiveAndDeleted);

                    if (wasDeleted) {
                        // remove() would have got rid of this so we have to add it back in
                        hashLookupLiveOnly.put(hash2, pos);
                        incrementSize();
                    }
                    return;
                }

                // key is not found
                long valueSize = valueLimit - valuePos;
                long pos = alloc(inBlocks(entrySize(keySize, valueSize)));
                long offset = offsetFromPos(pos);
                clearMetaData(offset);
                NativeBytes entry = entry(offset);

                keySizeMarshaller.writeSize(entry, keySize);

                // write the key
                entry.write(inBytes);

                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                valueSizeMarshaller.writeSize(entry, valueSize);
                alignment.alignPositionAddr(entry);

                // write the value
                inBytes.limit(valueLimit);
                inBytes.position(valuePos);

                entry.write(inBytes);

                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);

                incrementSize();

            } finally {
                writeUnlock();
            }
        }

        private boolean remoteKeyEquals(Bytes keyBytes, long keySize, Bytes entry) {
            return keySize == keySizeMarshaller.readSize(entry) && entry.startsWith(keyBytes);
        }

        V put(ThreadLocalCopies copies, MKI metaKeyWriter, KI keyWriter,
              K key, V value, long hash2, boolean replaceIfPresent,
              final byte identifier, final long timestamp) {
            writeLock();
            try {
                MultiMap hashLookup = hashLookupLiveAndDeleted;
                long keySize = metaKeyWriter.size(keyWriter, key);
                hashLookup.startSearch(hash2);
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    MultiStoreBytes entry = entry(offset);
                    if (!keyEquals(keyWriter, metaKeyWriter, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

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

                        final V prevValue = replaceValueOnPut(copies, key, value, entry, pos,
                                offset, !wasDeleted && !putReturnsNull, hashLookup);

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
                        return putReturnsNull ? null : readValue(copies, entry, null);
                    }
                }

                // key is not found
                long offset = putEntry(copies, metaKeyWriter, keyWriter, key, keySize, hash2, value,
                        identifier, timestamp, hashLookup);
                incrementSize();
                notifyPut(offset, true, key, value, posFromOffset(offset));
                return null;
            } finally {
                writeUnlock();
            }
        }

        /**
         * Used only with replication, its sometimes possible to receive an old ( or stale update )
         * from a remote map. This method is used to determine if we should ignore such updates.
         *  <p>We can reject put() and removes() when comparing times stamps with remote
         * systems
         *
         * @param entry      the maps entry
         * @param timestamp  the time the entry was created or updated
         * @param identifier the unique identifier relating to this map
         * @return true if the entry should not be processed
         */
        private boolean shouldIgnore(@NotNull final NativeBytes entry, final long timestamp,
                                     final byte identifier) {

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
         * Puts entry. If {@code value} implements {@link Byteable} interface and {@code usingValue}
         * is {@code true}, the value is backed with the bytes of this entry.
         *
         * @param hash2              a hash of the {@code keyBytes}. Caller was searching for the
         *                           key in the {@code searchedHashLookup} using this hash.
         * @param value              the value to put
         * @param identifier         the identifier of the outer CHM node
         * @param timestamp          the timestamp when the entry was put <s>(this could be later if
         *                           it was a remote put)</s> this method is called only from usual
         *                           put or acquire
         * @param searchedHashLookup the hash lookup that used to find the entry based on the key
         * @return offset of the written entry in the Segment bytes
         * @see VanillaChronicleMap.Segment#putEntry
         */
        private long putEntry(ThreadLocalCopies copies,
                              MKI metaKeyInterop, KI keyInterop, K key, long keySize, long hash2,
                              V value,
                              int identifier, long timestamp, MultiMap searchedHashLookup) {
            copies = valueWriterProvider.getCopies(copies);
            VW valueWriter = valueWriterProvider.get(copies, originalValueWriter);
            copies = metaValueWriterProvider.getCopies(copies);
            MetaBytesWriter<V, VW> metaValueWriter = metaValueWriterProvider.get(
                    copies, originalMetaValueWriter, valueWriter, value);

            return putEntry(metaKeyInterop, keyInterop, key, keySize, hash2,
                    identifier, timestamp, searchedHashLookup, metaValueWriter, valueWriter, value);
        }

        private <E, EW> long putEntry(
                MKI metaKeyInterop, KI keyInterop, K key, long keySize, long hash2,
                final int identifier, final long timestamp, MultiMap searchedHashLookup,
                MetaBytesWriter<E, EW> metaElemWriter, EW elemWriter, E elem) {
            long valueSize = metaElemWriter.size(elemWriter, elem);
            long entrySize = entrySize(keySize, valueSize);
            long pos = alloc(inBlocks(entrySize));
            long offset = offsetFromPos(pos);
            clearMetaData(offset);
            NativeBytes entry = entry(offset);

            keySizeMarshaller.writeSize(entry, keySize);
            metaKeyInterop.write(keyInterop, entry, key);

            entry.writeLong(timestamp);
            entry.writeByte(identifier);
            entry.writeBoolean(false);

            valueSizeMarshaller.writeSize(entry, valueSize);
            alignment.alignPositionAddr(entry);
            metaElemWriter.write(elemWriter, entry, elem);

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
         * @see VanillaChronicleMap.Segment#remove
         */
        V remove(ThreadLocalCopies copies, MKI metaKeyWriter, KI keyWriter,
                 K key, V expectedValue, long hash2,
                 final long timestamp, final byte identifier) {
            assert identifier > 0;
            writeLock();
            try {
                long keySize = metaKeyWriter.size(keyWriter, key);
                final MultiMap multiMap = hashLookupLiveAndDeleted;
                multiMap.startSearch(hash2);
                for (long pos; (pos = multiMap.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    MultiStoreBytes entry = entry(offset);
                    if (!keyEquals(keyWriter, metaKeyWriter, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

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

                    long valueSize = readValueSize(entry);
                    V valueRemoved = expectedValue != null || !removeReturnsNull
                            ? readValue(copies, entry, null, valueSize) : null;

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
                writeUnlock();
            }
        }

        @Override
        MultiMap containsKeyHashLookup() {
            return hashLookupLiveOnly;
        }

        /**
         * @see VanillaChronicleMap.Segment#replace
         */
        V replace(ThreadLocalCopies copies, MKI metaKeyWriter, KI keyWriter,
                  K key, V expectedValue, V newValue, long hash2, long timestamp) {
            writeLock();
            try {
                long keySize = metaKeyWriter.size(keyWriter, key);
                hashLookupLiveOnly.startSearch(hash2);
                for (long pos; (pos = hashLookupLiveOnly.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    MultiStoreBytes entry = entry(offset);
                    if (!keyEquals(keyWriter, metaKeyWriter, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    if (shouldIgnore(entry, timestamp, localIdentifier))
                        return null;
                    // skip the is deleted flag
                    entry.skip(1);

                    return onKeyPresentOnReplace(copies, key, expectedValue, newValue, pos, offset,
                            entry, hashLookupLiveOnly);
                }
                // key is not found
                return null;
            } finally {
                writeUnlock();
            }
        }

        @Override
        void replacePosInHashLookupOnRelocation(MultiMap searchedHashLookup,
                                                long prevPos, long pos) {
            searchedHashLookup.replacePrevPos(pos);
            long hash = searchedHashLookup.getSearchHash();
            MultiMap anotherLookup = searchedHashLookup == hashLookupLiveAndDeleted ?
                    hashLookupLiveOnly : hashLookupLiveAndDeleted;
            anotherLookup.replace(hash, prevPos, pos);
        }

        public void dirtyEntries(final long timeStamp,
                                 final ModificationIterator.EntryModifiableCallback callback) {

            this.readLock();
            try {
                final int index = Segment.this.getIndex();
                hashLookupLiveAndDeleted.forEach(new MultiMap.EntryConsumer() {

                    @Override
                    public void accept(long hash, long pos) {
                        final NativeBytes entry = entry(offsetFromPos(pos));
                        long keySize = keySizeMarshaller.readSize(entry);
                        entry.skip(keySize);

                        final long entryTimestamp = entry.readLong();

                        if (entryTimestamp >= timeStamp &&
                                entry.readByte() == ReplicatedChronicleMap.this.identifier())
                            callback.set(index, pos);
                    }
                });

            } finally {
                readUnlock();
            }
        }

        @Override
        public Entry<K, V> getEntry(long pos) {
            MultiStoreBytes entry = acquireTmpBytes();
            long offset = offsetFromPos(pos) + metaDataBytes;
            entry.setBytesOffset(bytes, offset);

            long keySize = keySizeMarshaller.readSize(entry);
            ThreadLocalCopies copies = keyReaderProvider.getCopies(null);
            K key = keyReaderProvider.get(copies, originalKeyReader).read(entry, keySize);

            long timestamp = entry.readLong();
            entry.skip(2L); // identifier and isDeleted flag

            long valueSize = valueSizeMarshaller.readSize(entry);
            alignment.alignPositionAddr(entry);
            copies = valueReaderProvider.getCopies(copies);
            V value = valueReaderProvider.get(copies, originalValueReader).read(entry, valueSize);

            return new TimestampTrackingEntry(key, value, timestamp);
        }

        @Override
        void clear() {
            // we have to make sure that every calls notifies on remove,
            // so that the replicators can pick it up
            for (K k : keySet()) {
                ReplicatedChronicleMap.this.remove(k);
            }
        }
    }

    class TimestampTrackingEntry extends SimpleEntry<K, V> {
        private static final long serialVersionUID = 0L;

        transient long timestamp;

        public TimestampTrackingEntry(K key, V value, long timestamp) {
            super(key, value);
            this.timestamp = timestamp;
        }

        @Override
        public V setValue(V value) {
            long newTimestamp = timestamp = timeProvider.currentTimeMillis();
            put(getKey(), value, localIdentifier, newTimestamp);
            return super.setValue(value);
        }
    }

    class EntryIterator extends VanillaChronicleMap<K, KI, MKI, V, VW, MVW>.EntryIterator {
        @Override
        void removePresent(VanillaChronicleMap.Segment seg, long pos) {
            @SuppressWarnings("unchecked")
            Segment segment = (Segment) seg;

            final long offset = segment.offsetFromPos(pos);
            final NativeBytes entry = segment.entry(offset);
            final long limit = entry.limit();

            final long keySize = keySizeMarshaller.readSize(entry);
            long keyPosition = entry.position();
            entry.skip(keySize);
            long timestamp = entry.readLong();
            entry.position(keyPosition);
            if (timestamp > ((TimestampTrackingEntry) returnedEntry).timestamp) {
                // The entry was updated after being returned from iterator.next()
                // Check that it is still the entry with the same key
                K key = returnedEntry.getKey();
                ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
                KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);
                copies = metaKeyInteropProvider.getCopies(copies);
                MKI metaKeyWriter =
                        metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);
                long returnedKeySize = metaKeyWriter.size(keyWriter, key);
                if (returnedKeySize != keySize ||
                        !metaKeyWriter.startsWith(keyWriter, entry, key)) {
                    // The case:
                    // 1. iterator.next() - thread 1
                    // 2. map.put() which cause relocation of the key, returned above - thread 2
                    // OR map.remove() which remove this key - thread 2
                    // 3. map.put() which place a new key on the `pos` in current segment - thread 3
                    // 4. iterator.remove() - thread 1
                    ReplicatedChronicleMap.this.remove(key);
                    return;
                }
            }
            final long segmentHash = segmentHash(hash(entry, keyPosition, keyPosition + keySize));

            segment.hashLookupLiveOnly.remove(segmentHash, pos);
            segment.decrementSize();

            entry.skip(keySize);
            entry.writeLong(timeProvider.currentTimeMillis());
            entry.writeByte(localIdentifier);
            entry.writeBoolean(true);

            segment.notifyRemoved(offset, returnedEntry.getKey(), returnedEntry.getValue(), pos);
        }
    }

    class EntrySet extends VanillaChronicleMap<K, KI, MKI, V, VW, MVW>.EntrySet {
        @NotNull
        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }
    }

    /**
     * receive an update from the map, via the MapEventListener and delegates the changes to the
     * currently active modification iterators
     */
    class ModificationDelegator extends MapEventListener<K, V, ChronicleMap<K, V>> {
        private static final long serialVersionUID = 0L;

        // the assigned modification iterators
        private final ATSDirectBitSet modIterSet;
        private final MapEventListener<K, V, ChronicleMap<K, V>> nextListener;
        private final AtomicReferenceArray<ModificationIterator> modificationIterators =
                new AtomicReferenceArray<ModificationIterator>(127 + RESERVED_MOD_ITER);

        public ModificationDelegator(@NotNull final MapEventListener<K, V, ChronicleMap<K, V>> nextListener,
                                     final Bytes bytes) {
            this.nextListener = nextListener;
            modIterSet = new ATSDirectBitSet(bytes);
        }

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
                modIterSet.set(remoteIdentifier);
                return newModificationIterator;
            }
        }

        @Override
        public void onPut(ChronicleMap<K, V> map, Bytes entry, int metaDataBytes,
                          boolean added, K key, V value, long pos, SharedSegment segment) {

            assert ReplicatedChronicleMap.this == map :
                    "ModificationIterator.onPut() is called from outside of the parent map";
            try {
                nextListener.onPut(map, entry, metaDataBytes, added, key, value, pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }

            for (long next = modIterSet.nextSetBit(0); next > 0; next = modIterSet.nextSetBit(next + 1)) {
                try {
                    final ModificationIterator modificationIterator = modificationIterators.get((int) next);
                    modificationIterator.onPut(map, entry, metaDataBytes,
                            added, key, value, pos, segment);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }


        }

        @Override
        public void onRemove(ChronicleMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, long pos, SharedSegment segment) {
            assert ReplicatedChronicleMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";
            try {
                nextListener.onRemove(map, entry, metaDataBytes, key, value, pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }

            for (long next = modIterSet.nextSetBit(0); next > 0; next = modIterSet.nextSetBit(next + 1)) {
                try {
                    final ModificationIterator modificationIterator = modificationIterators.get((int) next);
                    modificationIterator.onRemove(map, entry, metaDataBytes, key, value, pos, segment);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }

        }

        @Override
        void onRelocation(long pos, SharedSegment segment) {
            for (long next = modIterSet.nextSetBit(0); next > 0; next = modIterSet.nextSetBit(next + 1)) {
                try {
                    final ModificationIterator modificationIterator = modificationIterators.get((int) next);
                    modificationIterator.onRelocation(pos, segment);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        }

        @Override
        public void onGetFound(ChronicleMap<K, V> map, Bytes entry, int metaDataBytes,
                               K key, V value) {
            nextListener.onGetFound(map, entry, metaDataBytes, key, value);
        }

        /**
         * Always throws {@code NotSerializableException} since instances of this class are not
         * intended to be serializable.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new NotSerializableException(getClass().getCanonicalName());
        }

        /**
         * Always throws {@code NotSerializableException} since instances of this class are not
         * intended to be serializable.
         */
        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            throw new NotSerializableException(getClass().getCanonicalName());
        }


    }

    /**
     * <p>Once a change occurs to a map, map replication requires that these changes are picked up by
     * another thread, this class provides an iterator like interface to poll for such changes.
     * </p>
     * <p>In most cases the thread that adds data to the node is unlikely to be the same thread that
     * replicates the data over to the other nodes, so data will have to be marshaled between the
     * main thread storing data to the map, and the thread running the replication.
     * </p>
     * <p>One way to perform this marshalling, would be to pipe the data into a queue. However, This class
     * takes another approach. It uses a bit set, and marks bits which correspond to the indexes of
     * the entries that have changed. It then provides an iterator like interface to poll for such
     * changes.
     * </p>
     *
     * @author Rob Austin.
     */
    class ModificationIterator extends MapEventListener<K, V, ChronicleMap<K, V>>
            implements Replica.ModificationIterator {
        private static final long serialVersionUID = 0L;

        private final ModificationNotifier modificationNotifier;
        private final ATSDirectBitSet changes;
        private final int segmentIndexShift;
        private final long posMask;

        private final EntryModifiableCallback entryModifiableCallback = new EntryModifiableCallback();

        // records the current position of the cursor in the bitset
        private long position = -1L;

        /**
         * @param bytes                the back the bitset, used to mark which entries have changed
         * @param modificationNotifier called when ever there is a change applied
         */
        public ModificationIterator(@NotNull final Bytes bytes,
                                    @NotNull final ModificationNotifier modificationNotifier) {

            this.modificationNotifier = modificationNotifier;
            long bitsPerSegment = bitsPerSegmentInModIterBitSet();
            segmentIndexShift = Long.numberOfTrailingZeros(bitsPerSegment);
            posMask = bitsPerSegment - 1L;
            changes = new ATSDirectBitSet(bytes);
        }

        /**
         * used to merge multiple segments and positions into a single index used by the bit map
         *
         * @param segmentIndex the index of the maps segment
         * @param pos          the position within this {@code segmentIndex}
         * @return and index the has combined the {@code segmentIndex}  and  {@code pos} into a
         * single value
         */
        private long combine(int segmentIndex, long pos) {
            return (((long) segmentIndex) << segmentIndexShift) | pos;
        }

        @Override
        public void onPut(ChronicleMap<K, V> map, Bytes entry, int metaDataBytes,
                          boolean added, K key, V value, long pos, SharedSegment segment) {

            assert ReplicatedChronicleMap.this == map :
                    "ModificationIterator.onPut() is called from outside of the parent map";


            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();
        }

        @Override
        public void onRemove(ChronicleMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, long pos, SharedSegment segment) {
            assert ReplicatedChronicleMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";


            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();

        }

        /**
         * Ensures that garbage in the old entry's location won't be broadcast as changed entry.
         */
        @Override
        void onRelocation(long pos, SharedSegment segment) {
            changes.clear(combine(segment.getIndex(), pos));
            // don't call nextListener.onRelocation(),
            // because no one event listener else overrides this method.
        }


        /**
         * you can continue to poll hasNext() until data becomes available. If are are in the middle
         * of processing an entry via {@code nextEntry}, hasNext will return true until the bit is
         * cleared
         *
         * @return true if there is an entry
         */
        public boolean hasNext() {
            final long position = this.position;
            return changes.nextSetBit(position == NOT_FOUND ? 0L : position) != NOT_FOUND ||
                    (position > 0L && changes.nextSetBit(0L) != NOT_FOUND);
        }


        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        public boolean nextEntry(@NotNull final EntryCallback entryCallback, final int chronicleId) {
            long position = this.position;
            while (true) {
                long oldPosition = position;
                position = changes.nextSetBit(oldPosition + 1L);

                if (position == NOT_FOUND) {
                    if (oldPosition == NOT_FOUND) {
                        this.position = NOT_FOUND;
                        return false;
                    }
                    continue;
                }

                this.position = position;
                final SharedSegment segment = segment((int) (position >>> segmentIndexShift));
                segment.readLock();
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
                    segment.readUnlock();
                }
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

        /**
         * Always throws {@code NotSerializableException} since instances of this class are not
         * intended to be serializable.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new NotSerializableException(getClass().getCanonicalName());
        }

        /**
         * Always throws {@code NotSerializableException} since instances of this class are not
         * intended to be serializable.
         */
        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            throw new NotSerializableException(getClass().getCanonicalName());
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
            public void set(int segmentIndex, long pos) {
                final long combine = combine(segmentIndex, pos);
                changes.set(combine);
            }
        }
    }

}