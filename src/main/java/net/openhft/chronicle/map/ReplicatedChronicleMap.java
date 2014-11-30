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


import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.BytesBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.DelegatingMetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.openhft.chronicle.hash.serialization.Hasher.hash;
import static net.openhft.lang.MemoryUnit.*;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;

/**
 * <h2>A Replicating Multi Master HashMap</h2> <p>Each remote hash map, mirrors its changes over to another
 * remote hash map, neither hash map is considered the master store of data, each hash map uses timestamps to
 * reconcile changes. We refer to an instance of a remote hash-map as a node. A node will be connected to any
 * number of other nodes, for the first implementation the maximum number of nodes will be fixed. The data
 * that is stored locally in each node will become eventually consistent. So changes made to one node, for
 * example by calling put() will be replicated over to the other node. To achieve a high level of performance
 * and throughput, the call to put() won’t block, with concurrentHashMap, It is typical to check the return
 * code of some methods to obtain the old value for example remove(). Due to the loose coupling and lock free
 * nature of this multi master implementation,  this return value will only be the old value on the nodes
 * local data store. In other words the nodes are only concurrent locally. Its worth realising that another
 * node performing exactly the same operation may return a different value. However reconciliation will ensure
 * the maps themselves become eventually consistent. </p> <h2>Reconciliation </h2> <p>If two ( or more nodes )
 * were to receive a change to their maps for the same key but different values, say by a user of the maps,
 * calling the put(key, value). Then, initially each node will update its local store and each local store
 * will hold a different value, but the aim of multi master replication is to provide eventual consistency
 * across the nodes. So, with multi master when ever a node is changed it will notify the other nodes of its
 * change. We will refer to this notification as an event. The event will hold a timestamp indicating the time
 * the change occurred, it will also hold the state transition, in this case it was a put with a key and
 * value. Eventual consistency is achieved by looking at the timestamp from the remote node, if for a given
 * key, the remote nodes timestamp is newer than the local nodes timestamp, then the event from the remote
 * node will be applied to the local node, otherwise the event will be ignored. </p> <p>However there is an
 * edge case that we have to concern ourselves with, If two nodes update their map at the same time with
 * different values, we have to deterministically resolve which update wins, because of eventual consistency
 * both nodes should end up locally holding the same data. Although it is rare two remote nodes could receive
 * an update to their maps at exactly the same time for the same key, we have to handle this edge case, its
 * therefore important not to rely on timestamps alone to reconcile the updates. Typically the update with the
 * newest timestamp should win, but in this example both timestamps are the same, and the decision made to one
 * node should be identical to the decision made to the other. We resolve this simple dilemma by using a node
 * identifier, each node will have a unique identifier, the update from the node with the smallest identifier
 * wins. </p>
 *
 * @param <K> the entries key type
 * @param <V> the entries value type
 */
final class ReplicatedChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        extends VanillaChronicleMap<K, KI, MKI, V, VI, MVI>
        implements Replica, Replica.EntryExternalizable, Replica.EntryResolver<K, V> {
    // for file, jdbc and UDP replication
    public static final int RESERVED_MOD_ITER = 8;
    public static final int ADDITIONAL_ENTRY_BYTES = 10;
    private static final long serialVersionUID = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleMap.class);
    private static final long LAST_UPDATED_HEADER_SIZE = 128L * 8L;
    private final TimeProvider timeProvider;
    private final byte localIdentifier;
    transient Set<Closeable> closeables;
    private transient Bytes identifierUpdatedBytes;

    private transient ATSDirectBitSet modIterSet;
    private transient AtomicReferenceArray<ModificationIterator> modificationIterators;
    private transient long startOfModificationIterators;
    private boolean bootstrapOnlyLocalEntries;

    public ReplicatedChronicleMap(@NotNull AbstractChronicleMapBuilder<K, V, ?> builder,
                                  byte identifier)
            throws IOException {
        super(builder);
        this.timeProvider = builder.timeProvider();

        this.localIdentifier = identifier;
        this.bootstrapOnlyLocalEntries = builder.bootstrapOnlyLocalEntries;

        if (localIdentifier == -1) {
            throw new IllegalStateException("localIdentifier should not be -1");
        }

    }

    private int assignedModIterBitSetSizeInBytes() {
        return (int) CACHE_LINES.align(BYTES.alignAndConvert(127 + RESERVED_MOD_ITER, BITS), BYTES);
    }

    @Override
    VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment createSegment(
            NativeBytes segmentHeader, NativeBytes bytes, int index) {
        return new Segment(segmentHeader, bytes, index);
    }

    Class segmentType() {
        return Segment.class;
    }

    @Override
    void initTransients() {
        super.initTransients();
        modificationIterators =
                new AtomicReferenceArray<ModificationIterator>(127 + RESERVED_MOD_ITER);
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
        modIterSet = new ATSDirectBitSet(modDelBytes);
    }

    /**
     * @param segmentNum a unique index of the segment
     * @return the segment associated with the {@code segmentNum}
     */
    private Segment segment(int segmentNum) {
        return (Segment) segments[segmentNum];
    }

    private long currentTime() {
        return timeProvider.currentTimeMillis();
    }

    @Override
    void putBytes(ThreadLocalCopies copies, SegmentState segmentState, Bytes key, long keySize,
                  GetRemoteBytesValueInterops getRemoteBytesValueInterops, MultiStoreBytes value,
                  boolean replaceIfPresent, ReadValue<Bytes> readValue) {
        put(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, key, keySize, keyBytesToInstance,
                getRemoteBytesValueInterops, value, valueBytesToInstance,
                replaceIfPresent, readValue, false, localIdentifier, currentTime());;
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
    V put0(@NotNull K key, V value, boolean replaceIfPresent) {
        return put0(key, value, replaceIfPresent, localIdentifier, currentTime());
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
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        return put(copies, null, metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, value, valueIdentity(), replaceIfPresent, this, putReturnsNull,
                identifier, timeStamp);
    }

    @Override
    <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
            RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
    RV put(ThreadLocalCopies copies, SegmentState segmentState,
           MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
           InstanceOrBytesToInstance<KB, K> toKey,
           GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
           InstanceOrBytesToInstance<? super VB, V> toValue,
           boolean replaceIfPresent, ReadValue<RV> readValue, boolean resultUnused) {
        return put(copies, segmentState, metaKeyInterop, keyInterop, key, keySize, toKey,
                getValueInterops, value, toValue, replaceIfPresent, readValue, resultUnused,
                localIdentifier, currentTime());
    }

    @Override
    public void clear() {
        // we have to make sure that every calls notifies on remove,
        // so that the replicators can pick it up
        for (K k : keySet()) {
            ReplicatedChronicleMap.this.remove(k);
        }
    }

    private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
            RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
    RV put(ThreadLocalCopies copies, SegmentState segmentState,
           MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
           InstanceOrBytesToInstance<KB, K> toKey,
           GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
           InstanceOrBytesToInstance<? super VB, V> toValue,
           boolean replaceIfPresent, ReadValue<RV> readValue, boolean resultUnused,
           byte identifier, long timeStamp) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segment(segmentNum).put(copies, segmentState,
                metaKeyInterop, keyInterop, key, keySize, toKey,
                getValueInterops, value, toValue,
                segmentHash, replaceIfPresent, readValue, resultUnused,
                identifier, timeStamp);
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
    public Replica.ModificationIterator acquireModificationIterator(
            byte remoteIdentifier, @NotNull final ModificationNotifier modificationNotifier) {
        ModificationIterator modificationIterator = modificationIterators.get(remoteIdentifier);
        if (modificationIterator != null)
            return modificationIterator;

        synchronized (modificationIterators) {
            modificationIterator = modificationIterators.get(remoteIdentifier);

            if (modificationIterator != null)
                return modificationIterator;

            final Bytes bytes = ms.bytes(startOfModificationIterators +
                            (modIterBitSetSizeInBytes() * remoteIdentifier),
                    modIterBitSetSizeInBytes());

            final ModificationIterator newModificationIterator = new ModificationIterator(
                    bytes, modificationNotifier);

            modificationIterators.set(remoteIdentifier, newModificationIterator);
            modIterSet.set(remoteIdentifier);
            return newModificationIterator;
        }
    }

    @Override
    void onPut(VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).onPut(pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    @Override
    void onRemove(VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).onRemove(pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    @Override
    void onRelocation(VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).onRelocation(pos, segment);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling this method,
     * especially when being used in a multi threaded context.
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


        destination.writeByte(identifier);
        destination.writeBoolean(isDeleted);

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
     * This method does not set a segment lock, A segment lock should be obtained before calling this method,
     * especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(
            @NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
            @NotNull Bytes source) {

        final long keySize = keySizeMarshaller.readSize(source);
        final long valueSize = valueSizeMarshaller.readSize(source);
        final long timeStamp = source.readStopBit();


        final byte id = source.readByte();
        final boolean isDeleted = source.readBoolean();

        final byte remoteIdentifier;

        if (id != 0) {
            //     isDeleted = false;
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
                LOG.debug("READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})",
                        localIdentifier, remoteIdentifier, source.toString().trim()
                );
            }

            segment(segmentNum).remoteRemove(copies, segmentState,
                    source, segmentHash, timeStamp, remoteIdentifier);
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
        segment(segmentNum).remotePut(copies, segmentState, source,
                keySize, valueSize, segmentHash, remoteIdentifier, timeStamp);
        setLastModificationTime(remoteIdentifier, timeStamp);

        if (debugEnabled) {
            source.limit(valueLimit);
            source.position(valuePosition);
            LOG.debug(message + "value=" + source.toString().trim() + ")");
        }


    }

    @Override
    Set<Entry<K, V>> newEntrySet() {
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

    class Segment extends VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment {

        Segment(NativeBytes segmentHeader, NativeBytes bytes, int index) {
            super(segmentHeader, bytes, index);
        }

        @Override
        long entrySize(long keySize, long valueSize) {
            long result = alignment.alignAddr(metaDataBytes +
                    keySizeMarshaller.sizeEncodingSize(keySize) + keySize + ADDITIONAL_ENTRY_BYTES +
                    valueSizeMarshaller.sizeEncodingSize(valueSize)) + valueSize;
            // replication enforces that the entry size will never be larger than an unsigned short
            if (result > Integer.MAX_VALUE)
                throw new IllegalStateException("ENTRY WRITE_BUFFER_SIZE TOO LARGE : Replicated " +
                        "ChronicleMap's" +
                        " are restricted to an " +
                        "entry size of " + Integer.MAX_VALUE + ", " +
                        "your entry size=" + result);

            return result;
        }

        @Override
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV acquireWithoutLock(
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, boolean create) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            SegmentState localSegmentState = segmentState;
            try {
                MultiStoreBytes entry = null;
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (localSegmentState == null) {
                            copies = SegmentState.getCopies(copies);
                            localSegmentState = SegmentState.get(copies);
                        }
                        entry = localSegmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);
                    entry.skip(ADDITIONAL_ENTRY_BYTES - 1L);
                    boolean isDeleted = entry.readBoolean();
                    if (isDeleted) {
                        if (!create)
                            return readValue.readNull();

                        long valueSizePos = entry.position();

                        entry.position(valueSizePos - ADDITIONAL_ENTRY_BYTES);
                        // todo theoretically, currentTime() call should be outside locking
                        // because locking might take time > 1 ms. but making current time a param
                        // of acquireWithoutLock would complicate code much, requiring putting
                        // lookupUsing() back into ReplicatedChMap, and other methods
                        entry.writeLong(currentTime());
                        entry.writeByte(localIdentifier);
                        // deleted flag
                        entry.writeBoolean(false);

                        long prevValueSize = readValueSize(entry);
                        long valueAddr = entry.positionAddr();
                        long entryEndAddr = valueAddr + prevValueSize;

                        // todo add api which doesn't require key instance
                        K keyInstance = toKey.toInstance(copies, key, keySize);
                        MetaBytesWriter metaElemWriter;
                        Object elemWriter;
                        Object elem;
                        if (defaultValueProvider != null) {
                            V defaultValue = defaultValueProvider.get(keyInstance);
                            elem = defaultValue;
                            VI valueInterop =
                                    valueInteropProvider.get(copies, originalValueInterop);
                            elemWriter = valueInterop;
                            metaElemWriter = metaValueInteropProvider.get(
                                    copies, originalMetaValueInterop, valueInterop, defaultValue);
                        } else if (prepareValueBytesAsWriter != null) {
                            elem = keyInstance;
                            elemWriter = null;
                            metaElemWriter = prepareValueBytesAsWriter;
                        } else {
                            throw defaultValueOrPrepareBytesShouldBeSpecified();
                        }
                        putValue(pos, offset, entry, valueSizePos, entryEndAddr,
                                copies, localSegmentState, metaElemWriter, elemWriter, elem,
                                metaElemWriter.size(elemWriter, elem), hashLookup);
                        pos = localSegmentState.pos;

                        incrementSize();
                        hashLookup.putPosition(pos);


                        entry.position(valueSizePos);
                        long valueSize = readValueSize(entry);
                        long valuePos = entry.position();
                        RV v = readValue.readValue(copies, entry, usingValue, valueSize);

                        // put callbacks
                        onPut(this, pos);
                        if (bytesEventListener != null) {
                            bytesEventListener.onPut(entry, 0L, metaDataBytes, valuePos, true);
                        }
                        if (eventListener != null) {
                            V valueInstance = toValue.toInstance(copies, v, valueSize);
                            eventListener.onPut(keyInstance, valueInstance, null);
                        }

                        return v;
                    } else {
                        return readValueAndNotifyGet(copies, key, keySize, toKey,
                                readValue, usingValue, toValue, entry);
                    }
                }
                if (!create)
                    return readValue.readNull();
                if (entry == null) {
                    if (localSegmentState == null) {
                        copies = SegmentState.getCopies(copies); // after this, copies is not null
                        localSegmentState = SegmentState.get(copies);
                    }
                    entry = localSegmentState.tmpBytes;
                }
                return createEntryOnAcquire(copies, localSegmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        readValue, usingValue, toValue, entry);
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
        }

        /**
         * called from a remote node as part of replication
         */
        void remoteRemove(
                @NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                Bytes keyBytes, long hash2, final long timestamp, final byte identifier) {
            writeLock(null);
            try {
                ReadValueToBytes readValueToLazyBytes = segmentState.readValueToLazyBytes;
                readValueToLazyBytes.valueSizeMarshaller(valueSizeMarshaller);
                Boolean removed = (Boolean) removeWithoutLock(copies, segmentState,
                        DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                        BytesBytesInterop.INSTANCE, keyBytes, keyBytes.remaining(),
                        keyBytesToInstance, null, null, outputValueBytesToInstance,
                        hash2, readValueToLazyBytes, true,
                        timestamp, identifier, true, true);
                if (!removed && LOG.isDebugEnabled()) {
                    LOG.debug("Segment.remoteRemove() : key=" + keyBytes.toString().trim() +
                            " was not found (or the remote update is late)");
                }
            } finally {
                writeUnlock();
            }
        }

        /**
         * called from a remote node when it wishes to propagate a remove event
         */
        void remotePut(@NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                       @NotNull final Bytes entry, long keySize, long valueSize, long hash2,
                       final byte identifier, final long timestamp) {
            GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                    segmentState.getRemoteBytesValueInterops;
            MultiStoreBytes value = getRemoteBytesValueInterops.getValueBytes(
                    entry, entry.position() + keySize);
            getRemoteBytesValueInterops.valueSize(valueSize);
            ReadValueToBytes readValueToLazyBytes = segmentState.readValueToLazyBytes;
            readValueToLazyBytes.valueSizeMarshaller(valueSizeMarshaller);

            writeLock(null);
            try {
                putWithoutLock(copies, segmentState,
                        DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                        BytesBytesInterop.INSTANCE, entry, keySize, keyBytesToInstance,
                        getRemoteBytesValueInterops, value, valueBytesToInstance,
                        hash2, true, readValueToLazyBytes, true, identifier, timestamp,
                        true);
            } finally {
                writeUnlock();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV put(@Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
               MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
               InstanceOrBytesToInstance<KB, K> toKey,
               GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
               InstanceOrBytesToInstance<? super VB, V> toValue,
               long hash2, boolean replaceIfPresent,
               ReadValue<RV> readValue, boolean resultUnused,
               byte identifier, long timeStamp) {
            writeLock(null);
            try {
                return putWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, value, toValue,
                        hash2, replaceIfPresent, readValue, resultUnused,
                        identifier, timeStamp, false);
            } finally {
                if (segmentState != null)
                    segmentState.close();
                writeUnlock();
            }
        }

        @Override
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV putWithoutLock(
                @Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue, long hash2,
                boolean replaceIfPresent, ReadValue<RV> readValue, boolean resultUnused) {
            return putWithoutLock(copies, segmentState, metaKeyInterop, keyInterop, key, keySize,
                    toKey, getValueInterops, value, toValue, hash2, replaceIfPresent, readValue,
                    resultUnused, localIdentifier, currentTime(), false);
        }

        private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV putWithoutLock(
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                long hash2, boolean replaceIfPresent,
                ReadValue<RV> readValue, boolean resultUnused,
                byte identifier, long timestamp, boolean remote) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            SegmentState localSegmentState = segmentState;
            if (localSegmentState == null) {
                copies = SegmentState.getCopies(copies); // copies is not null now
                localSegmentState = SegmentState.get(copies);
            }
            try {
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = localSegmentState.tmpBytes;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    final long timeStampPosAddr = entry.positionAddr();

                    if (shouldIgnore(entry, timestamp, identifier)) {
                        // the following assert should be enabled, but TimeBasedReplicationTest
                        // intentionally violates the explained invariant, => assertion fails.
                        // todo do something with this

                        // we should ignore only external remote updates
                        // which don't use put and remove results
                        // assert resultUnused;
                        return null;
                    }

                    boolean isDeleted = entry.readBoolean();

                    if (replaceIfPresent || isDeleted) {
                        entry.positionAddr(timeStampPosAddr);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // deleted flag
                        entry.writeBoolean(false);

                        RV prevValue = replaceValueAndNotifyPut(copies, localSegmentState,
                                key, keySize, toKey,
                                getValueInterops, value, toValue,
                                entry, pos, offset, hashLookup, readValue,
                                resultUnused, isDeleted, remote);
                        // for DRY (reusing replaceValueAndNotifyPut() method),
                        // size is updated AFTER callbacks are called.
                        // however this shouldn't be an issue because exclusive segment lock
                        // is still held
                        if (isDeleted) {
                            incrementSize();
                            hashLookup.putPosition(localSegmentState.pos);
                        }
                        if (resultUnused)
                            return null;
                        return isDeleted ? readValue.readNull() : prevValue;
                    } else {
                        long valueSize = readValueSize(entry);
                        return resultUnused ? null :
                                readValue.readValue(copies, entry, null, valueSize);
                    }
                }
                // key is not found
                VBI valueInterop = getValueInterops.getValueInterop(copies);
                MVBI metaValueInterop =
                        getValueInterops.getMetaValueInterop(copies, valueInterop, value);
                long valueSize = metaValueInterop.size(valueInterop, value);
                putEntry(localSegmentState, metaKeyInterop, keyInterop, key, keySize,
                        metaValueInterop, valueInterop, value, entry, false);
                entry.position(localSegmentState.valueSizePos - ADDITIONAL_ENTRY_BYTES);
                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                // put callbacks
                if (!remote)
                    onPut(this, localSegmentState.pos);
                if (bytesEventListener != null)
                    bytesEventListener.onPut(entry, 0L, metaDataBytes,
                            localSegmentState.valueSizePos, true);
                if (eventListener != null)
                    eventListener.onPut(toKey.toInstance(copies, key, keySize),
                            toValue.toInstance(copies, value, valueSize), null);

                return resultUnused ? null : readValue.readNull();
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
        }

        /**
         * Used only with replication, its sometimes possible to receive an old ( or stale update )
         * from a remote map. This method is used to determine if we should ignore such updates.
         *
         * <p>We can reject put() and removes() when comparing times stamps with remote systems
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

        @Override
        void manageReplicationBytes(Bytes entry, boolean writeDefaultInitialReplicationValues,
                                    boolean remove) {
            if (!writeDefaultInitialReplicationValues) {
                entry.skip(ADDITIONAL_ENTRY_BYTES);
            } else {
                entry.writeLong(currentTime());
                entry.writeByte(localIdentifier);
                entry.writeBoolean(remove);
            }
        }

        @Override
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
        Object remove(@Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                      MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                      InstanceOrBytesToInstance<KB, K> toKey,
                      GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                      InstanceOrBytesToInstance<RV, V> toValue,
                      long hash2, ReadValue<RV> readValue, boolean resultUnused) {
            writeLock(null);
            try {
                return removeWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, expectedValue, toValue,
                        hash2, readValue, resultUnused, currentTime(), localIdentifier, false,
                        expectedValue != null);
            } finally {
                if (segmentState != null)
                    segmentState.close();
                writeUnlock();
            }
        }

        @Override
        boolean isDeleted(Bytes entry, long keySize) {
            return entry.readBoolean(entry.position() + keySize + ADDITIONAL_ENTRY_BYTES - 1L);
        }

        @Override
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
        Object removeWithoutLock(
                @Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, ReadValue<RV> readValue, boolean resultUnused) {
            return removeWithoutLock(copies, segmentState, metaKeyInterop, keyInterop, key, keySize,
                    toKey, getValueInterops, expectedValue, toValue, hash2, readValue, resultUnused,
                    currentTime(), localIdentifier, false, expectedValue != null);
        }

        /**
         * - if expectedValue is not null, returns Boolean.TRUE (removed) or
         * Boolean.FALSE (entry not found), regardless the expectedValue object is Bytes instance
         * (RPC call) or the value instance
         * - if expectedValue is null:
         *   - if resultUnused is false, null or removed value is returned
         *   - if resultUnused is true, null is always returned
         */
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
        Object removeWithoutLock(
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, ReadValue<RV> readValue, boolean resultUnused,
                long timestamp, byte identifier, boolean remote, boolean booleanResult) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            assert identifier > 0;
            expectedValueNotNullImpliesBooleanResult(expectedValue, booleanResult);
            SegmentState localSegmentState = segmentState;
            try {
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = null;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (localSegmentState == null) {
                            copies = SegmentState.getCopies(copies); // copies is not null now
                            localSegmentState = SegmentState.get(copies);
                        }
                        entry = localSegmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    long timestampPos = entry.position();
                    if (shouldIgnore(entry, timestamp, identifier)) {
                        // the following assert should be enabled, but TimeBasedReplicationTest
                        // intentionally violates the explained invariant, => assertion fails.
                        // todo do something with this

                        // we should ignore only remote updates
                        // which don't use remove, put results
                        // assert booleanResult || resultUnused;
                        return booleanResult ? Boolean.FALSE : null;
                    }
                    boolean isDeleted = entry.readBoolean();
                    if (isDeleted) {
                        entry.position(timestampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        break; // to "key not found" location
                    }

                    long valueSizePos = entry.position();
                    long valueSize = readValueSize(entry);
                    long valuePos = entry.position();

                    // check the value assigned for the key is that we expect
                    if (expectedValue != null) {
                        VBI valueInterop = getValueInterops.getValueInterop(copies);
                        MVBI metaValueInterop = getValueInterops.getMetaValueInterop(
                                copies, valueInterop, expectedValue);
                        if (metaValueInterop.size(valueInterop, expectedValue) != valueSize)
                            return Boolean.FALSE;
                        if (!metaValueInterop.startsWith(valueInterop, entry, expectedValue))
                            return Boolean.FALSE;
                    }

                    entry.position(timestampPos);
                    entry.writeLong(timestamp);
                    entry.writeByte(identifier);
                    entry.writeBoolean(true);
                    entry.position(valuePos);

                    return removeEntry(copies, key, keySize, toKey, toValue, readValue,
                            resultUnused, hashLookup, entry, pos, valueSizePos,
                            valueSize, remote, false, booleanResult);
                }
                // key is not found
                if (booleanResult) {
                    return Boolean.FALSE;
                } else {
                    return resultUnused ? null : readValue.readNull();
                }
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        Object replace(
                @Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getExpectedValueInterops, VB expectedValue,
                GetValueInterops<VB, VBI, MVBI> getNewValueInterops, VB newValue,
                ReadValue<RV> readValue, InstanceOrBytesToInstance<? super RV, V> toValue,
                long hash2) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            long timestamp = currentTime();
            byte identifier = localIdentifier;
            writeLock(null);
            try {
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = null;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (segmentState == null) {
                            copies = SegmentState.getCopies(copies);
                            segmentState = SegmentState.get(copies);
                        }
                        entry = segmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    long timestampPos = entry.position();
                    if (shouldIgnore(entry, timestamp, identifier)) {
                        LOG.error("Trying to replace a value for key={} on the node with id={} " +
                                "at time={} (current time), but the entry is updated by node " +
                                "with id={} at time={}. Time is not monotonic across nodes!?",
                                key, identifier, timestamp, entry.readByte(entry.position() - 1),
                                entry.readLong(entry.position() - ADDITIONAL_ENTRY_BYTES + 1));
                        return readValue.readNull();
                    }
                    boolean isDeleted = entry.readBoolean();
                    if (isDeleted)
                        break;

                    Object result = onKeyPresentOnReplace(copies, segmentState, key, keySize, toKey,
                            getExpectedValueInterops, expectedValue, getNewValueInterops, newValue,
                            readValue, toValue, pos, offset, entry, hashLookup);
                    if (result != Boolean.FALSE) {
                        entry.position(timestampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                    }
                    return result;
                }
                // key is not found
                return readValue.readNull();
            } finally {
                if (segmentState != null)
                    segmentState.close();
                writeUnlock();
            }
        }

        /**
         * @param bootstrapOnlyLocalEntries if true - only entries that have been create from
         *                                  this node will be published during a bootstrap
         */
        public void dirtyEntries(final long timeStamp,
                                 final ModificationIterator.EntryModifiableCallback callback,
                                 final boolean bootstrapOnlyLocalEntries) {

            readLock(null);
            ThreadLocalCopies copies = SegmentState.getCopies(null);
            try (SegmentState segmentState = SegmentState.get(copies)) {
                final int index = Segment.this.getIndex();
                final MultiStoreBytes tmpBytes = segmentState.tmpBytes;
                hashLookup().forEach(new MultiMap.EntryConsumer() {

                    @Override
                    public void accept(long hash, long pos) {
                        final NativeBytes entry = reuse(tmpBytes, offsetFromPos(pos));
                        long keySize = keySizeMarshaller.readSize(entry);
                        entry.skip(keySize);

                        final long entryTimestamp = entry.readLong();

                        if (entryTimestamp >= timeStamp && (!bootstrapOnlyLocalEntries ||
                                entry.readByte() == ReplicatedChronicleMap.this.identifier()))
                            callback.set(index, pos);
                    }
                });
            } finally {
                readUnlock();
            }
        }

        @Override
        public Entry<K, V> getEntry(@NotNull SegmentState segmentState, long pos) {
            Bytes entry = reuse(segmentState.tmpBytes, offsetFromPos(pos));

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
        boolean isDeleted(long pos) {
            bytes.position(offsetFromPos(pos) + metaDataBytes);
            long keySize = keySizeMarshaller.readSize(bytes);
            return bytes.readBoolean(bytes.position() + keySize + ADDITIONAL_ENTRY_BYTES - 1L);
        }
    }

    @Override
    void shouldNotBeCalledFromReplicatedChronicleMap(String method) {
        throw new AssertionError(method + "() method should not be called by " +
                "ReplicatedChronicleMap instance");
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
            long newTimestamp = timestamp = currentTime();
            put(getKey(), value, localIdentifier, newTimestamp);
            return super.setValue(value);
        }
    }

    class EntryIterator extends VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.EntryIterator {

        @Override
        public Entry<K, V> next() {
            return super.next();
        }

        @Override
        void removePresent(VanillaChronicleMap.Segment seg, long pos) {
            @SuppressWarnings("unchecked")
            Segment segment = (Segment) seg;

            final long offset = segment.offsetFromPos(pos);
            final NativeBytes entry = segment.reuse(this.entry, offset);

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
                KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
                copies = metaKeyInteropProvider.getCopies(copies);
                MKI metaKeyInterop =
                        metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
                long returnedKeySize = metaKeyInterop.size(keyInterop, key);
                if (returnedKeySize != keySize ||
                        !metaKeyInterop.startsWith(keyInterop, entry, key)) {
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

            removePresent(segment, pos, entry, keySize, 0L, false);
        }
    }

    class EntrySet extends VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.EntrySet {
        @NotNull
        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }
    }

    /**
     * <p>Once a change occurs to a map, map replication requires that these changes are picked up by another
     * thread, this class provides an iterator like interface to poll for such changes. </p> <p>In most cases
     * the thread that adds data to the node is unlikely to be the same thread that replicates the data over
     * to the other nodes, so data will have to be marshaled between the main thread storing data to the map,
     * and the thread running the replication. </p> <p>One way to perform this marshalling, would be to pipe
     * the data into a queue. However, This class takes another approach. It uses a bit set, and marks bits
     * which correspond to the indexes of the entries that have changed. It then provides an iterator like
     * interface to poll for such changes. </p>
     *
     * @author Rob Austin.
     */
    class ModificationIterator implements Replica.ModificationIterator {
        private final ModificationNotifier modificationNotifier;
        private final ATSDirectBitSet changes;
        private final int segmentIndexShift;
        private final long posMask;

        private final EntryModifiableCallback entryModifiableCallback =
                new EntryModifiableCallback();

        // records the current position of the cursor in the bitset
        private long position = -1L;

        // todo get rid of this
        private final MultiStoreBytes tmpBytes = new MultiStoreBytes();

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
         * @return and index the has combined the {@code segmentIndex}  and  {@code pos} into a single value
         */
        private long combine(int segmentIndex, long pos) {
            return (((long) segmentIndex) << segmentIndexShift) | pos;
        }

        public void onPut(long pos, SharedSegment segment) {
            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();
        }

        public void onRemove(long pos, SharedSegment segment) {
            changes.set(combine(segment.getIndex(), pos));
            modificationNotifier.onChange();
        }

        /**
         * Ensures that garbage in the old entry's location won't be broadcast as changed entry.
         */
        void onRelocation(long pos, SharedSegment segment) {
            changes.clear(combine(segment.getIndex(), pos));
        }

        /**
         * you can continue to poll hasNext() until data becomes available. If are are in the middle of
         * processing an entry via {@code nextEntry}, hasNext will return true until the bit is cleared
         *
         * @return true if there is an entry
         */
        @Override
        public boolean hasNext() {
            final long position = this.position;
            return changes.nextSetBit(position == NOT_FOUND ? 0L : position) != NOT_FOUND ||
                    (position > 0L && changes.nextSetBit(0L) != NOT_FOUND);
        }

        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        @Override
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
                final VanillaChronicleMap.Segment segment =
                        segment((int) (position >>> segmentIndexShift));
                segment.readLock(null);
                try {
                    if (changes.clearIfSet(position)) {

                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        final NativeBytes entry =
                                segment.reuse(tmpBytes, segment.offsetFromPos(segmentPos));

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
                segment.dirtyEntries(fromTimeStamp, entryModifiableCallback,
                        bootstrapOnlyLocalEntries);
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
            public void set(int segmentIndex, long pos) {
                final long combine = combine(segmentIndex, pos);
                changes.set(combine);
            }
        }
    }
}