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
import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.HashReplicableEntry;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.impl.*;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
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
public class ReplicatedChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R>
        extends VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R>
        implements Replica, Replica.EntryExternalizable {
    // for file, jdbc and UDP replication
    public static final int RESERVED_MOD_ITER = 8;
    public static final int ADDITIONAL_ENTRY_BYTES = 10;
    private static final long serialVersionUID = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleMap.class);
    private static final long LAST_UPDATED_HEADER_SIZE = 128L * 8L;
    public final TimeProvider timeProvider;
    private final byte localIdentifier;
    transient Set<Closeable> closeables;
    private transient Bytes identifierUpdatedBytes;

    private transient ATSDirectBitSet modIterSet;
    private transient AtomicReferenceArray<ModificationIterator> modificationIterators;
    private transient long startOfModificationIterators;
    private boolean bootstrapOnlyLocalEntries;
    
    // TODO init in builder
    public transient MapRemoteOperations<K, V, R> remoteOperations =
            DefaultSpi.mapRemoteOperations();
    transient CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> remoteOpContext;
    transient CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> remoteItContext;

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

    @Override
    void initQueryContext() {
        queryCxt = new ThreadLocal<
                CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?>>() {
            @Override
            protected CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?>
            initialValue() {
                return new CompiledReplicatedMapQueryContext<>(ReplicatedChronicleMap.this);
            }
        };
    }

    @Override
    void initIterationContext() {
        iterCxt = new ThreadLocal<
                CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?>>() {
            @Override
            protected CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?>
            initialValue() {
                return new CompiledReplicatedMapIterationContext<>(ReplicatedChronicleMap.this);
            }
        };
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

    public void raiseChange(long segmentIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).raiseChange(segmentIndex, pos);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    public void dropChange(long segmentIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                modificationIterators.get((int) next).dropChange(segmentIndex, pos);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    @Override
    public boolean identifierCheck(@NotNull HashReplicableEntry<?> entry, int chronicleId) {
        return entry.originIdentifier() == localIdentifier;
    }

    @Override
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
    
    private CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> q() {
        //noinspection unchecked
        return (CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?>) queryCxt.get();
    }

    @Override
    public CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> mapContext() {
        CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> q = q().getContext();
        q.initUsed(true);
        return q;
    }

    /**
     * Assumed to be called from a single thread - the replication thread. Not to waste time
     * for going into replication thread's threadLocal map, cache the context in Map's field
     */
    private CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> remoteOpContext() {
        if (remoteOpContext == null) {
            remoteOpContext = q();
        }
        assert !remoteOpContext.usedInit();
        remoteOpContext.initUsed(true);
        return remoteOpContext;
    }

    private CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> remoteItContext() {
        if (remoteItContext == null) {
            remoteItContext = i();
        }
        assert !remoteItContext.usedInit();
        remoteItContext.initUsed(true);
        return remoteItContext;
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(@NotNull Bytes source) {
        try (CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R, ?> remoteOpContext =
                     mapContext()) {
            remoteOpContext.initReplicationInput(source);
            //remoteOpContext.processReplicatedEvent();
        }
    }

    private CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> i() {
        //noinspection unchecked
        return (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?>) iterCxt.get();
    }

    public CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> iterationContext() {
        CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> c = i().getContext();
        c.initUsed(true);
        return c;
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
                try (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> context =
                        iterationContext()) {
                    context.initTheSegmentIndex(segmentIndex);
                    context.updateLock().lock();
                    if (changesForUpdates.get(position)) {

                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        context.initEntry(segmentPos);

                        // if the entry should be ignored, we'll move the next entry
                        if (entryCallback.shouldBeIgnored(context, chronicleId)) {
                            changesForUpdates.clear(position);
                            continue;
                        }

                        // it may not be successful if the buffer can not be re-sized so we will
                        // process it later, by NOT clearing the changes.clear(position)
                        context.entryBytes().limit(context.valueOffset() + context.valueSize());
                        context.entryBytes().position(context.keySizeOffset());
                        boolean success = entryCallback.onEntry(context.entryBytes(), chronicleId);
                        entryCallback.onAfterEntry();

                        if (success)
                            changesForUpdates.clear(position);

                        return success;
                    }
                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in onRelocation()),
                    // go to pick up next (next iteration in while (true) loop)
                }
            }
        }

        @Override
        public void dirtyEntries(long fromTimeStamp) {
            try (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R, ?> c =
                         iterationContext()) {
                // iterate over all the segments and mark bit in the modification iterator
                // that correspond to entries with an older timestamp
                for (int i = 0; i < actualSegments; i++) {
                    final int segmentIndex = i;
                    c.initTheSegmentIndex(segmentIndex);
                    c.forEachRemoving(entry -> {
                        MapReplicableEntry re = (MapReplicableEntry) entry;
                        assert re.originTimestamp() > 0L;
                        if (re.originIdentifier() >= fromTimeStamp &&
                                (!bootstrapOnlyLocalEntries ||
                                        re.originIdentifier() == localIdentifier)) {
                            raiseChange(segmentIndex, c.pos());
                        }
                        return true;
                    });
                }
            }
        }
    }
}