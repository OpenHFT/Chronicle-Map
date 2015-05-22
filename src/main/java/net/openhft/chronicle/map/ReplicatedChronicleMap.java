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

import net.openhft.chronicle.hash.KeyContext;
import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.impl.ContextFactory;
import net.openhft.chronicle.hash.impl.HashContext;
import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.BytesBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.DelegatingMetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.openhft.lang.MemoryUnit.*;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;

/**
 * <h2>A Replicating Multi Master HashMap</h2> <p>Each remote hash map, mirrors its changes over to
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
 * reconciliation will ensure the maps themselves become eventually consistent. </p>
 * <h2>Reconciliation </h2> <p>If two ( or more nodes ) were to receive a change to their maps for
 * the same key but different values, say by a user of the maps, calling the put(key, value). Then,
 * initially each node will update its local store and each local store will hold a different value,
 * but the aim of multi master replication is to provide eventual consistency across the nodes. So,
 * with multi master when ever a node is changed it will notify the other nodes of its change. We
 * will refer to this notification as an event. The event will hold a timestamp indicating the time
 * the change occurred, it will also hold the state transition, in this case it was a put with a key
 * and value. Eventual consistency is achieved by looking at the timestamp from the remote node, if
 * for a given key, the remote nodes timestamp is newer than the local nodes timestamp, then the
 * event from the remote node will be applied to the local node, otherwise the event will be
 * ignored. </p> <p>However there is an edge case that we have to concern ourselves with, If two
 * nodes update their map at the same time with different values, we have to deterministically
 * resolve which update wins, because of eventual consistency both nodes should end up locally
 * holding the same data. Although it is rare two remote nodes could receive an update to their maps
 * at exactly the same time for the same key, we have to handle this edge case, its therefore
 * important not to rely on timestamps alone to reconcile the updates. Typically the update with the
 * newest timestamp should win, but in this example both timestamps are the same, and the decision
 * made to one node should be identical to the decision made to the other. We resolve this simple
 * dilemma by using a node identifier, each node will have a unique identifier, the update from the
 * node with the smallest identifier wins. </p>
 *
 * @param <K> the entries key type
 * @param <V> the entries value type
 */
class ReplicatedChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
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

    public ReplicatedChronicleMap(@NotNull ChronicleMapBuilder<K, V> builder,
                                  AbstractReplication replication)
            throws IOException {
        super(builder, true);
        this.timeProvider = builder.timeProvider();

        this.localIdentifier = replication.identifier();
        this.bootstrapOnlyLocalEntries = replication.bootstrapOnlyLocalEntries();

        if (localIdentifier == -1) {
            throw new IllegalStateException("localIdentifier should not be -1");
        }
    }

    private int assignedModIterBitSetSizeInBytes() {
        return (int) CACHE_LINES.align(BYTES.alignAndConvert(127 + RESERVED_MOD_ITER, BITS), BYTES);
    }

    @Override
    public void initTransients() {
        super.initTransients();
        ownInitTransients();
    }

    private void ownInitTransients() {
        modificationIterators =
                new AtomicReferenceArray<>(127 + RESERVED_MOD_ITER);
        closeables = new CopyOnWriteArraySet<>();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        ownInitTransients();
    }

    long modIterBitSetSizeInBytes() {
        long bytes = BITS.toBytes(bitsPerSegmentInModIterBitSet() * actualSegments);
        return CACHE_LINES.align(bytes, BYTES);
    }

    private long bitsPerSegmentInModIterBitSet() {
        // min 128 * 8 to prevent false sharing on updating bits from different segments
        // TODO this doesn't prevent false sharing. There should be GAPS between per-segment bits
        return Maths.nextPower2(actualChunksPerSegment, 128L * 8L);
    }

    @Override
    public long mapHeaderInnerSize() {
        return super.mapHeaderInnerSize() + LAST_UPDATED_HEADER_SIZE +
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
    public void onHeaderCreated() {
        long offset = super.mapHeaderInnerSize();

        identifierUpdatedBytes = ms.bytes(offset, LAST_UPDATED_HEADER_SIZE).zeroOut();
        offset += LAST_UPDATED_HEADER_SIZE;

        Bytes modDelBytes = ms.bytes(offset, assignedModIterBitSetSizeInBytes()).zeroOut();
        offset += assignedModIterBitSetSizeInBytes();
        startOfModificationIterators = offset;
        modIterSet = new ATSDirectBitSet(modDelBytes);
    }

    @Override
    public void clear() {
        forEachEntry(KeyContext::remove);
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

    void raiseChange(long segmentIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).raiseChange(segmentIndex, pos);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    void dropChange(long segmentIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).dropChange(segmentIndex, pos);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    public boolean identifierCheck(@NotNull Bytes entry, int chronicleId) {
        long start = entry.position();
        try {
            final long keySize = keySizeMarshaller.readSize(entry);
            entry.skip(keySize + 8); // we skip 8 for the timestamp
            final byte identifier = entry.readByte();
            return identifier == localIdentifier;
        } finally {
            entry.position(start);
        }
    }

    static class ReplicatedContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends VanillaContext<K, KI, MKI, V, VI, MVI> {

        ReplicatedContext() {
            super();
        }

        ReplicatedContext(HashContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI> rm() {
            return (ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI>) m();
        }

        @Override
        public void totalCheckClosed() {
            super.totalCheckClosed();
            assert !replicationStateInit() : "replication state not closed";
            assert !replicationUpdateInit() : "replication update not closed";
        }

        /////////////////////////////////////////////////
        // Replication state
        long replicationBytesOffset;
        long timestamp;
        byte identifier;

        @Override
        public boolean containsKey0() {
            return searchStatePresent() && !isDeleted();
        }

        @Override
        public void keyFound() {
            super.keyFound();
            initReplicationBytesOffset0();
            timestamp = entry.readLong(replicationBytesOffset);
            identifier = entry.readByte(replicationBytesOffset + 8L);
        }

        boolean isDeleted() {
            return entry.readBoolean(replicationBytesOffset + 9L);
        }

        void initReplicationBytesOffset0() {
            replicationBytesOffset = keyOffset0() + keySize0();
        }

        @Override
        public void initKeyOffset0() {
            super.initKeyOffset0();
            initReplicationBytesOffset0();
        }

        void closeReplicationState() {
            if (!replicationStateInit())
                return;
            closeReplicationState0();
        }

        boolean replicationStateInit() {
            return replicationBytesOffset != 0;
        }

        void closeReplicationState0() {
            replicationBytesOffset = 0L;
            timestamp = 0L;
            identifier = (byte) 0;
        }

        @Override
        public void closeKeySearchDependants() {
            super.closeKeySearchDependants();
            closeReplicationState();
        }

        @Override
        void initValueSizeOffset0() {
            valueSizeOffset = replicationBytesOffset + ADDITIONAL_ENTRY_BYTES;
        }

        /////////////////////////////////////////////////
        // Replication update
        long newTimestamp;
        byte newIdentifier;

        void initReplicationUpdate() {
            if (replicationUpdateInit())
                return;
            checkHashInit();
            initReplicationUpdate0();
        }

        boolean replicationUpdateInit() {
            return newIdentifier != 0;
        }

        void initReplicationUpdate0() {
            newTimestamp = rm().timeProvider.currentTime();
            newIdentifier = rm().identifier();
        }

        boolean remoteUpdate() {
            return newIdentifier != rm().identifier();
        }

        void closeReplicationUpdate() {
            if (!replicationUpdateInit())
                return;
            closeReplicationUpdate0();
        }

        void closeReplicationUpdate0() {
            newTimestamp = 0L;
            newIdentifier = (byte) 0;
        }

        @Override
        public void initRemoveDependencies() {
            super.initRemoveDependencies();
            initReplicationUpdate();
        }

        void writeReplicationBytes() {
            entry.writeLong(replicationBytesOffset, newTimestamp);
            entry.writeByte(replicationBytesOffset + 8L, newIdentifier);
        }

        void writeDeleted() {
            entry.writeBoolean(replicationBytesOffset + 9L, true);
        }

        void writePresent() {
            entry.writeBoolean(replicationBytesOffset + 9L, false);
        }

        @Override
        long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
            return super.sizeOfEverythingBeforeValue(keySize, valueSize) + ADDITIONAL_ENTRY_BYTES;
        }

        @Override
        boolean put0() {
            try {
                if (!searchStatePresent()) {
                    putEntry();
                    writePresent();

                } else {
                    if (!shouldIgnore()) {
                        boolean deleted = isDeleted();
                        initValueBytes();
                        putValue();
                        if (deleted) {
                            deleted(deleted() - 1);
                            writePresent();
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            } finally {
                closeReplicationUpdate();
            }
        }

        @Override
        void beforeRelocation() {
            rm().dropChange(segmentIndex, pos);
        }

        @Override
        void initPutDependencies() {
            super.initPutDependencies();
            initReplicationUpdate();
        }

        @Override
        void writeNewValueAndSwitch() {
            super.writeNewValueAndSwitch();
            writeReplicationBytes();
            updateChange();
            timestamp = newTimestamp;
            identifier = newIdentifier;
        }

        class PseudoMetaBytesInterop implements MetaBytesInterop<Object, Object> {

            @Override
            public boolean startsWith(Object interop, Bytes bytes, Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <I2> boolean equivalent(
                    Object interop, Object o,
                    MetaBytesInterop<Object, I2> otherMetaInterop, I2 otherInterop, Object other) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long hash(Object interop, LongHashFunction hashFunction, Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long size(Object writer, Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(Object writer, Bytes bytes, Object o) {
                bytes.skip(newValueSize);
            }
        }

        final MetaBytesInterop<V, VI> pseudoMetaValueInterop =
                (MetaBytesInterop<V, VI>) new PseudoMetaBytesInterop();

        @Override
        public boolean remove0() {
            if (containsKey()) {
                if (!shouldIgnore()) {
                    writeReplicationBytes();
                    writeDeleted();
                    updateChange();
                    deleted(deleted() + 1);
                    return true;

                } else {
                    return false;
                }
            } else {
                if (pos >= 0L) {
                    if (!shouldIgnore()) {
                        writeReplicationBytes();
                        updateChange();
                    }
                } else {
                    newValue = (V) pseudoMetaValueInterop;
                    metaValueInterop = (MVI) pseudoMetaValueInterop;
                    newValueSize = m().valueSizeMarshaller.minEncodableSize();
                    put0();
                    deleted(deleted() + 1);
                    writeDeleted();
                    closeNewValue();
                }
                return false;
            }
        }

        void updateChange() {
            if (remoteUpdate()) {
                rm().dropChange(segmentIndex, pos);

            } else {
                rm().raiseChange(segmentIndex, pos);
            }
        }

        @Override
        public void closeRemove() {
            closeReplicationUpdate();
        }

        private boolean testTimeStampInSensibleRange() {
            if (rm().timeProvider == TimeProvider.SYSTEM) {
                long currentTime = TimeProvider.SYSTEM.currentTime();
                assert Math.abs(currentTime - timestamp) <= 100000000 :
                        "unrealistic timestamp: " + timestamp;
                assert Math.abs(currentTime - newTimestamp) <= 100000000 :
                        "unrealistic newTimestamp: " + newTimestamp;
            }
            return true;
        }

        boolean shouldIgnore() {
            assert replicationStateInit() : "replication state not init";
            assert replicationUpdateInit() : "replication update not init";
            assert testTimeStampInSensibleRange();
            if (!remoteUpdate()) {
                newTimestamp = timestamp + 1L;
                return false;
            }
            return timestamp > newTimestamp ||
                    (timestamp == newTimestamp && identifier > newIdentifier);
        }

        public void dirtyEntries(final long dirtyFromTimeStamp,
                                 final ReplicatedChronicleMap.ModificationIterator modIter,
                                 final boolean bootstrapOnlyLocalEntries) {
            readLock().lock();
            try {
                hashLookup.forEach((hash, pos) -> {
                    ReplicatedContext.this.pos = pos;
                    initKeyFromPos();
                    keyFound();
                    assert timestamp > 0L;
                    if (timestamp >= dirtyFromTimeStamp && (!bootstrapOnlyLocalEntries ||
                            identifier == rm().identifier()))
                        modIter.raiseChange(segmentIndex, pos);
                });
            } finally {
                readLock().unlock();
            }
        }
    }

    enum ReplicatedContextFactory implements ContextFactory<ReplicatedContext> {
        INSTANCE;

        @Override
        public ReplicatedContext createContext(HashContext root, int indexInContextCache) {
            return new ReplicatedContext(root, indexInContextCache);
        }

        @Override
        public ReplicatedContext createRootContext() {
            return new ReplicatedContext();
        }

        @Override
        public Class<ReplicatedContext> contextClass() {
            return ReplicatedContext.class;
        }
    }

    @Override
    ReplicatedContext<K, KI, MKI, V, VI, MVI> rawContext() {
        return VanillaContext.get(ReplicatedContextFactory.INSTANCE);
    }

    @Override
    VanillaContext rawBytesContext() {
        return VanillaContext.get(BytesReplicatedContextFactory.INSTANCE);
    }

    static class BytesReplicatedContext
            extends ReplicatedContext<Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>,
            Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>> {
        BytesReplicatedContext() {
            super();
        }

        BytesReplicatedContext(HashContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        @Override
        public void initKeyModel0() {
            initBytesKeyModel0();
        }

        @Override
        public void initKey0(Bytes key) {
            initBytesKey0(key);
        }

        @Override
        void initValueModel0() {
            initBytesValueModel0();
        }

        @Override
        void initNewValue0(Bytes newValue) {
            initNewBytesValue0(newValue);
        }

        @Override
        void closeValue0() {
            closeBytesValue0();
        }

        @Override
        public Bytes get() {
            return getBytes();
        }

        @Override
        public Bytes getUsing(Bytes usingValue) {
            return getBytesUsing(usingValue);
        }
    }

    enum BytesReplicatedContextFactory implements ContextFactory<BytesReplicatedContext> {
        INSTANCE;

        @Override
        public BytesReplicatedContext createContext(HashContext root, int indexInContextCache) {
            return new BytesReplicatedContext(root, indexInContextCache);
        }

        @Override
        public BytesReplicatedContext createRootContext() {
            return new BytesReplicatedContext();
        }

        @Override
        public Class<BytesReplicatedContext> contextClass() {
            return BytesReplicatedContext.class;
        }
    }

    @Override
    VanillaContext<K, KI, MKI, V, VI, MVI> acquireContext(K key) {
        ReplicatedAcquireContext<K, KI, MKI, V, VI, MVI> c =
                VanillaContext.get(ReplicatedAcquireContextFactory.INSTANCE);
        c.initHash(this);
        c.initKey(key);
        return c;
    }

    static class ReplicatedAcquireContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends ReplicatedContext<K, KI, MKI, V, VI, MVI> {
        ReplicatedAcquireContext() {
            super();
        }

        ReplicatedAcquireContext(HashContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        @Override
        public boolean put(V newValue) {
            return acquirePut(newValue);
        }

        @Override
        public boolean remove() {
            return acquireRemove();
        }

        @Override
        public void close() {
            acquireClose();
        }
    }

    enum ReplicatedAcquireContextFactory implements ContextFactory<ReplicatedAcquireContext> {
        INSTANCE;

        @Override
        public ReplicatedAcquireContext createContext(HashContext root,
                                            int indexInContextCache) {
            return new ReplicatedAcquireContext(root, indexInContextCache);
        }

        @Override
        public ReplicatedAcquireContext createRootContext() {
            return new ReplicatedAcquireContext();
        }

        @Override
        public Class<ReplicatedAcquireContext> contextClass() {
            return ReplicatedAcquireContext.class;
        }
    }

    public int sizeOfEntry(@NotNull Bytes entry, int chronicleId) {
        long start = entry.position();
        try {
            final long keySize = keySizeMarshaller.readSize(entry);

            entry.skip(keySize + 8); // we skip 8 for the timestamp

            final byte identifier = entry.readByte();
            if (identifier != localIdentifier) {
                // although unlikely, this may occur if the entry has been updated
                return 0;
            }

            entry.skip(1); // is Deleted
            long valueSize = valueSizeMarshaller.readSize(entry);

            alignment.alignPositionAddr(entry);
            long result = (entry.position() + valueSize - start);

            // entries can be larger than Integer.MAX_VALUE as we are restricted to the size we can
            // make a byte buffer
            assert result < Integer.MAX_VALUE;

            return (int) result;
        } finally {
            entry.position(start);
        }
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void writeExternalEntry(@NotNull Bytes entry,
                                   @NotNull Bytes destination,
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
            valueSize = valueSizeMarshaller.minEncodableSize();
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
        destination.write(entry, entry.position(), entry.remaining());

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
        destination.write(entry, entry.position(), entry.remaining());

        if (debugEnabled) {
            LOG.debug(message + "value=" + entry.toString().trim() + ")");
        }
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(@NotNull BytesReplicatedContext context,
            @NotNull Bytes source) {
        // TODO cache contexts per map -- don't init map on each readExternalEntry()
        context.initHash(this);
        try {
            final long keySize = keySizeMarshaller.readSize(source);
            final long valueSize = valueSizeMarshaller.readSize(source);
            final long timeStamp = source.readStopBit();

            final byte id = source.readByte();
            final boolean isDeleted = source.readBoolean();

            final byte remoteIdentifier;

            if (id != 0) {
                remoteIdentifier = id;

            } else {
                throw new IllegalStateException("identifier can't be 0");
            }

            if (remoteIdentifier == ReplicatedChronicleMap.this.identifier()) {
                // this may occur when working with UDP, as we may receive our own data
                return;
            }

            context.newTimestamp = timeStamp;
            context.newIdentifier = remoteIdentifier;

            final long keyPosition = source.position();
            final long keyLimit = keyPosition + keySize;
            source.limit(keyLimit);

            context.metaKeyInterop = DelegatingMetaBytesInterop.instance();
            context.keySize = keySize;
            context.initBytesKey00(source);

            boolean debugEnabled = LOG.isDebugEnabled();

            context.updateLock().lock();
            if (isDeleted) {
                if (debugEnabled) {
                    LOG.debug("READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})",
                            localIdentifier, remoteIdentifier, source.toString().trim()
                    );
                }
                context.remove();
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

            // compute hash => segment and locate the entry
            context.containsKey();

            context.metaValueInterop = DelegatingMetaBytesInterop.instance();
            context.newValueSize = valueSize;
            source.limit(valueLimit);
            source.position(valuePosition);
            context.initNewBytesValue00(source);

            context.initPutDependencies();
            context.put0();
            setLastModificationTime(remoteIdentifier, timeStamp);

            if (debugEnabled) {
                LOG.debug(message + "value=" + source.toString().trim() + ")");
            }
        } finally {
            context.closeHash();
        }
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

    /**
     * <p>Once a change occurs to a map, map replication requires that these changes are picked up
     * by another thread, this class provides an iterator like interface to poll for such changes.
     * </p> <p>In most cases the thread that adds data to the node is unlikely to be the same thread
     * that replicates the data over to the other nodes, so data will have to be marshaled between
     * the main thread storing data to the map, and the thread running the replication. </p> <p>One
     * way to perform this marshalling, would be to pipe the data into a queue. However, This class
     * takes another approach. It uses a bit set, and marks bits which correspond to the indexes of
     * the entries that have changed. It then provides an iterator like interface to poll for such
     * changes. </p>
     *
     * @author Rob Austin.
     */
    class ModificationIterator implements Replica.ModificationIterator {
        private final ModificationNotifier modificationNotifier;
        private final SingleThreadedDirectBitSet changesForUpdates;
        // to getVolatile when reading changes bits, because we iterate when without lock.
        // hardly this is needed on x86, probably on other architectures too.
        // Anyway getVolatile is cheap.
        private final ATSDirectBitSet changesForIteration;
        private final int segmentIndexShift;
        private final long posMask;
        private final ReplicatedContext<K, KI, MKI, V, VI, MVI> context =
                (ReplicatedContext<K, KI, MKI, V, VI, MVI>) mapContext();

        // records the current position of the cursor in the bitset
        private volatile long position = -1L;

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
            changesForUpdates = new SingleThreadedDirectBitSet(bytes);
            changesForIteration = new ATSDirectBitSet(bytes);
        }

        /**
         * used to merge multiple segments and positions into a single index used by the bit map
         *
         * @param segmentIndex the index of the maps segment
         * @param pos          the position within this {@code segmentIndex}
         * @return and index the has combined the {@code segmentIndex}  and  {@code pos} into a
         * single value
         */
        private long combine(long segmentIndex, long pos) {
            return (segmentIndex << segmentIndexShift) | pos;
        }

        void raiseChange(long segmentIndex, long pos) {
            changesForUpdates.set(combine(segmentIndex, pos));
            modificationNotifier.onChange();
        }

        void dropChange(long segmentIndex, long pos) {
            changesForUpdates.clear(combine(segmentIndex, pos));
        }

        /**
         * you can continue to poll hasNext() until data becomes available. If are are in the middle
         * of processing an entry via {@code nextEntry}, hasNext will return true until the bit is
         * cleared
         *
         * @return true if there is an entry
         */
        @Override
        public boolean hasNext() {
            final long position = this.position;
            return changesForIteration.nextSetBit(position == NOT_FOUND ? 0L : position)
                    != NOT_FOUND ||
                    (position > 0L && changesForIteration.nextSetBit(0L) != NOT_FOUND);
        }

        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        @Override
        public boolean nextEntry(@NotNull EntryCallback entryCallback, int chronicleId) {
            long position = this.position;
            while (true) {
                long oldPosition = position;
                position = changesForIteration.nextSetBit(oldPosition + 1L);

                if (position == NOT_FOUND) {
                    if (oldPosition == NOT_FOUND) {
                        this.position = NOT_FOUND;
                        return false;
                    }
                    continue;
                }

                this.position = position;
                int segmentIndex = (int) (position >>> segmentIndexShift);
                if (context.segmentIndex != segmentIndex) {
                    context.closeSegmentIndex();
                    context.segmentIndex = segmentIndex;
                    context.initLocks();
                    context.initSegment();
                }
                context.updateLock().lock();
                try {
                    if (changesForUpdates.get(position)) {
                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        context.reuse(segmentPos);

                        // if the entry should be ignored, we'll move the next entry
                        if (entryCallback.shouldBeIgnored(context.entry, chronicleId)) {
                            changesForUpdates.clear(position);
                            continue;
                        }

                        // it may not be successful if the buffer can not be re-sized so we will
                        // process it later, by NOT clearing the changes.clear(position)
                        boolean success = entryCallback.onEntry(context.entry, chronicleId);
                        entryCallback.onAfterEntry();

                        if (success)
                            changesForUpdates.clear(position);

                        return success;
                    }
                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in onRelocation()),
                    // go to pick up next (next iteration in while (true) loop)
                } finally {
                    context.updateLock().unlock();
                }
            }
        }

        @Override
        public void dirtyEntries(long fromTimeStamp) {
            try (ReplicatedContext<K, KI, MKI, V, VI, MVI> context =
                         (ReplicatedContext<K, KI, MKI, V, VI, MVI>) mapContext()) {
                // iterate over all the segments and mark bit in the modification iterator
                // that correspond to entries with an older timestamp
                for (int i = 0; i < actualSegments; i++) {
                    context.segmentIndex = i;
                    context.initSegment();
                    try {
                        context.dirtyEntries(fromTimeStamp, this, bootstrapOnlyLocalEntries);
                    } finally {
                        context.closeSegmentIndex();
                    }
                }
            }
        }
    }
}