/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.bitset.BitSetFrame;
import net.openhft.chronicle.algo.bitset.ConcurrentFlatBitSetFrame;
import net.openhft.chronicle.algo.bitset.SingleThreadedFlatBitSetFrame;
import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.hash.impl.TierCountersArea;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.replication.AbstractReplication;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.impl.CompiledReplicatedMapIterationContext;
import net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static net.openhft.chronicle.algo.bytes.Access.nativeAccess;
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
    private static final int SIZE_OF_BOOTSTRAP_TIME_STAMP = 8;

    public transient TimeProvider timeProvider;
    /**
     * Default value is 0, that corresponds to "unset" identifier value (valid ids are positive)
     */
    private transient byte localIdentifier;
    transient Set<Closeable> closeables;
    private transient Bytes identifierUpdatedBytes;

    private transient ATSDirectBitSet modIterSet;
    private transient AtomicReferenceArray<ModificationIterator> modificationIterators;
    private transient long startOfModificationIterators;
    private transient boolean bootstrapOnlyLocalEntries;

    public transient long cleanupTimeout;
    public transient TimeUnit cleanupTimeoutUnit;
    
    public transient MapRemoteOperations<K, V, R> remoteOperations;
    transient CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R> remoteOpContext;
    transient CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R> remoteItContext;

    transient BitSetFrame mainSegmentsModIterFrameForUpdates;
    transient BitSetFrame mainSegmentsModIterFrameForIteration;
    transient BitSetFrame tierBulkModIterFrameForUpdates;
    transient BitSetFrame tierBulkModIterFrameForIteration;

    public ReplicatedChronicleMap(@NotNull ChronicleMapBuilder<K, V> builder,
                                  AbstractReplication replication)
            throws IOException {
        super(builder);
        initTransientsFromReplication(replication);
    }

    @Override
    protected VanillaGlobalMutableState createGlobalMutableState() {
        return DataValueClasses.newDirectReference(ReplicatedGlobalMutableState.class);
    }

    @Override
    protected ReplicatedGlobalMutableState globalMutableState() {
        return (ReplicatedGlobalMutableState) super.globalMutableState();
    }

    private int assignedModIterBitSetSizeInBytes() {
        return (int) CACHE_LINES.align(BYTES.alignAndConvert(127 + RESERVED_MOD_ITER, BITS), BYTES);
    }

    @Override
    public void initTransients() {
        super.initTransients();
        initOwnTransients();
    }

    private void initOwnTransients() {
        modificationIterators =
                new AtomicReferenceArray<>(127 + RESERVED_MOD_ITER);
        closeables = new CopyOnWriteArraySet<>();
        long mainSegmentsBitSetSize = BYTES.toBits(modIterBitSetSizeInBytes());
        mainSegmentsModIterFrameForUpdates =
                new SingleThreadedFlatBitSetFrame(mainSegmentsBitSetSize);
        mainSegmentsModIterFrameForIteration =
                new ConcurrentFlatBitSetFrame(mainSegmentsBitSetSize);

        long tierBulkBitSetSize =
                BYTES.toBits(tierBulkModIterBitSetSizeInBytes(tiersInBulk));
        tierBulkModIterFrameForUpdates = new SingleThreadedFlatBitSetFrame(tierBulkBitSetSize);
        tierBulkModIterFrameForIteration = new ConcurrentFlatBitSetFrame(tierBulkBitSetSize);
    }

    @Override
    void initTransientsFromBuilder(ChronicleMapBuilder<K, V> builder) {
        super.initTransientsFromBuilder(builder);
        this.remoteOperations = (MapRemoteOperations<K, V, R>) builder.remoteOperations;
        this.timeProvider = builder.timeProvider();
        cleanupTimeout = builder.cleanupTimeout;
        cleanupTimeoutUnit = builder.cleanupTimeoutUnit;
    }

    void initTransientsFromReplication(AbstractReplication replication) {
        this.localIdentifier = replication.identifier();
        this.bootstrapOnlyLocalEntries = replication.bootstrapOnlyLocalEntries();
        if (localIdentifier == -1)
            throw new IllegalStateException("localIdentifier should not be -1");
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initOwnTransients();
    }

    long modIterBitSetSizeInBytes() {
        long bytes = BITS.toBytes(bitsPerSegmentInModIterBitSet() * actualSegments);
        return CACHE_LINES.align(bytes, BYTES);
    }

    @Override
    protected long computeTierBulkInnerOffsetToTiers(long tiersInBulk) {
        return super.computeTierBulkInnerOffsetToTiers(tiersInBulk) +
                tierBulkModIterBitSetSizeInBytes(tiersInBulk) * (128 + RESERVED_MOD_ITER);
    }

    private long tierBulkModIterBitSetSizeInBytes(long numberOfTiersInBulk) {
        long bytes = BITS.toBytes(bitsPerSegmentInModIterBitSet() * numberOfTiersInBulk);
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
                assignedModIterBitSetSizeInBytes() +
                (modIterBitSetSizeInBytes() * (128 + RESERVED_MOD_ITER));
    }

    public void setLastModificationTime(byte identifier, long timestamp) {
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

        identifierUpdatedBytes = ms.bytes(offset, LAST_UPDATED_HEADER_SIZE);
        offset += LAST_UPDATED_HEADER_SIZE;

        Bytes modDelBytes = ms.bytes(offset, assignedModIterBitSetSizeInBytes());
        offset += assignedModIterBitSetSizeInBytes();
        startOfModificationIterators = offset;
        modIterSet = new ATSDirectBitSet(modDelBytes);
    }

    @Override
    protected void zeroOutNewlyMappedChronicleMapBytes() {
        super.zeroOutNewlyMappedChronicleMapBytes();
        bytes.zeroOut(super.mapHeaderInnerSize(), this.mapHeaderInnerSize(), true);
    }

    void addCloseable(Closeable closeable) {
        closeables.add(closeable);
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
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
        byte id = localIdentifier;
        if (id == 0) {
            throw new IllegalStateException("Replication identifier is not set for this\n" +
                    "replicated Chronicle Map. This should only be possible if persisted\n" +
                    "replicated Chronicle Map access from another process/JVM run/after\n" +
                    "a transfer from another machine, and replication identifier is not\n" +
                    "specified when access is configured, e. g. ChronicleMap.of(...)" +
                    ".createPersistedTo(existingFile).\n" +
                    "In this case, replicated Chronicle Map \"doesn't know\" it's identifier,\n" +
                    "and is able to perform simple _read_ operations like map.get(), which\n" +
                    "doesn't access the identifier. To perform updates, insertions, replication\n" +
                    "tasks, you should configure the current node identifier,\n" +
                    "by `replication(identifier)` method call in ChronicleMapBuilder\n" +
                    "configuration chain.");
        }
        assert id > 0;
        return id;
    }

    @Override
    public ModificationIterator acquireModificationIterator(byte remoteIdentifier) {
        ModificationIterator modificationIterator = modificationIterators.get(remoteIdentifier);
        if (modificationIterator != null)
            return modificationIterator;

        synchronized (modificationIterators) {
            modificationIterator = modificationIterators.get(remoteIdentifier);

            if (modificationIterator != null)
                return modificationIterator;

            final ModificationIterator modIter = new ModificationIterator(remoteIdentifier);
            modificationIterators.set(remoteIdentifier, modIter);
            modIterSet.set(remoteIdentifier);
            return modIter;
        }
    }

    public void raiseChange(long tierIndex, long pos, long timestamp) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                acquireModificationIterator((byte) next).raiseChange(tierIndex, pos, timestamp);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    public void dropChange(long tierIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                acquireModificationIterator((byte) next).dropChange(tierIndex, pos);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    public void moveChange(long oldTierIndex, long oldPos, long newTierIndex, long newPos,
                           long timestamp) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            try {
                ModificationIterator modificationIterator =
                        acquireModificationIterator((byte) next);
                if (modificationIterator.dropChange(oldTierIndex, oldPos))
                    modificationIterator.raiseChange(newTierIndex, newPos, timestamp);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    public boolean isChanged(long tierIndex, long pos) {
        for (long next = modIterSet.nextSetBit(0L); next > 0L;
             next = modIterSet.nextSetBit(next + 1L)) {
            ModificationIterator modificationIterator =
                    acquireModificationIterator((byte) next);
            if (modificationIterator.isChanged(tierIndex, pos))
                return true;
        }
        return false;
    }

    @Override
    public boolean identifierCheck(@NotNull ReplicableEntry entry, int chronicleId) {
        return entry.originIdentifier() == identifier();
    }

    @Override
    public int sizeOfEntry(@NotNull Bytes entry, int chronicleId) {

        long start = entry.position();
        try {
            final long keySize = keySizeMarshaller.readSize(entry);

            entry.skip(keySize + 8); // we skip 8 for the timestamp

            final byte identifier = entry.readByte();
            if (identifier != identifier()) {
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

            return (int) result + SIZE_OF_BOOTSTRAP_TIME_STAMP;
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
                                   int chronicleId,
                                   long bootstrapTime) {

        final long keySize = keySizeMarshaller.readSize(entry);

        final long keyPosition = entry.position();
        entry.skip(keySize);
        final long timeStamp = entry.readLong();

        final byte identifier = entry.readByte();
        if (identifier != identifier()) {
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
        destination.writeLong(bootstrapTime);
        keySizeMarshaller.writeSize(destination, keySize);
        valueSizeMarshaller.writeSize(destination, valueSize);
        destination.writeStopBit(timeStamp);

        if (identifier == 0)
            throw new IllegalStateException("Identifier can't be 0");
        destination.writeByte(identifier);
        destination.writeBoolean(isDeleted);

        // write the key
        entry.position(keyPosition);
        destination.write(entry, entry.position(), keySize);

        boolean debugEnabled = LOG.isDebugEnabled();
        String message = null;
        if (debugEnabled) {
            if (isDeleted) {
                LOG.debug("WRITING ENTRY TO DEST -  into local-id={}, remove(key={})",
                        identifier(), entry.toString().trim());
            } else {
                message = String.format(
                        "WRITING ENTRY TO DEST  -  into local-id=%d, put(key=%s,",
                        identifier(), entry.toString().trim());
            }
        }

        if (isDeleted)
            return;

        entry.position(valuePosition);
        // skipping the alignment, as alignment wont work when we send the data over the wire.
        alignment.alignPositionAddr(entry);

        // writes the value
        destination.write(entry, entry.position(), valueSize);

        if (debugEnabled) {
            LOG.debug(message + "value=" + entry.toString().trim() + ")");
        }
    }
    
    private ChainingInterface q() {
        ChainingInterface queryContext;
        queryContext = cxt.get();
        if (queryContext == null) {
            queryContext = new CompiledReplicatedMapQueryContext<>(ReplicatedChronicleMap.this);
            cxt.set(queryContext);
        }
        return queryContext;
    }

    @Override
    public CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R> mapContext() {
        return q().getContext(CompiledReplicatedMapQueryContext.class,
                ci -> new CompiledReplicatedMapQueryContext<>(ci, this));
    }

    /**
     * Assumed to be called from a single thread - the replication thread. Not to waste time
     * for going into replication thread's threadLocal map, cache the context in Map's field
     */
    private CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R> remoteOpContext() {
        if (remoteOpContext == null) {
            remoteOpContext = (CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R>) q();
        }
        assert !remoteOpContext.usedInit();
        remoteOpContext.initUsed(true);
        return remoteOpContext;
    }

    private CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R> remoteItContext() {
        if (remoteItContext == null) {
            remoteItContext =
                    (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R>) i();
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
        try (CompiledReplicatedMapQueryContext<K, KI, MKI, V, VI, MVI, R> remoteOpContext =
                     mapContext()) {
            remoteOpContext.initReplicationInput(source);
            remoteOpContext.processReplicatedEvent();
        }
    }

    private ChainingInterface i() {
        ChainingInterface iterContext;
        iterContext = cxt.get();
        if (iterContext == null) {
            iterContext = new CompiledReplicatedMapIterationContext<>(ReplicatedChronicleMap.this);
            cxt.set(iterContext);
        }
        return iterContext;
    }

    public CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R> iterationContext() {
        return i().getContext(CompiledReplicatedMapIterationContext.class,
                ci -> new CompiledReplicatedMapIterationContext<>(ci, this));
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
        private ModificationNotifier modificationNotifier;

        private final long mainSegmentsChangesBitSetAddr;
        private final int segmentIndexShift;
        private final long posMask;

        private final long offsetToBitSetWithinATierBulk;

        // when a bootstrap is send this is the time stamp that the client will bootstrap up to
        // if it is set as ZERO then the onPut() will set it to the current time, once the
        // consumer has cycled through the bit set the timestamp will be set back to zero.
        private AtomicLong bootStrapTimeStamp = new AtomicLong();
        private long lastBootStrapTimeStamp = timeProvider.currentTime();

        // records the current position of the cursor in the bitset
        // TODO volatile?
        private volatile long position = NOT_FOUND;

        // -1 for main segments area, 0-N is tier bulk id
        private int iterationMainSegmentsAreaOrTierBulk = -1;
        private long tierBulkBitSetAddr = 0L;

        public ModificationIterator(int remoteIdentifier) {
            long bitsPerSegment = bitsPerSegmentInModIterBitSet();
            segmentIndexShift = Long.numberOfTrailingZeros(bitsPerSegment);
            posMask = bitsPerSegment - 1L;
            mainSegmentsChangesBitSetAddr = ms.address() + startOfModificationIterators +
                    remoteIdentifier * modIterBitSetSizeInBytes();
            nativeAccess().zeroOut(null, mainSegmentsChangesBitSetAddr, modIterBitSetSizeInBytes());
            offsetToBitSetWithinATierBulk =
                    remoteIdentifier * tierBulkModIterBitSetSizeInBytes(tiersInBulk);
        }

        public ModificationIterator(int remoteIdentifier, ModificationNotifier notifier) {
            this(remoteIdentifier);
            setModificationNotifier(notifier);
        }

        public void setModificationNotifier(ModificationNotifier modificationNotifier) {
            this.modificationNotifier = modificationNotifier;
        }

        /**
         * used to merge multiple segments and positions into a single index used by the bit map
         *
         * @param tierIndex the index of the maps segment
         * @param pos          the position within this {@code tierIndex}
         * @return and index the has combined the {@code tierIndex}  and  {@code pos} into a
         * single value
         */
        private long combine(long tierIndex, long pos) {
            long tierIndexMinusOne = tierIndex - 1;
            if (tierIndexMinusOne < actualSegments)
                return (tierIndexMinusOne << segmentIndexShift) | pos;
            return combineExtraTier(tierIndexMinusOne, pos);
        }

        private long combineExtraTier(long tierIndexMinusOne, long pos) {
            long extraTierIndex = tierIndexMinusOne - actualSegments;
            long tierIndexOffsetWithinBulk = extraTierIndex & (tiersInBulk - 1);
            return (tierIndexOffsetWithinBulk << segmentIndexShift) | pos;
        }

        void raiseChange(long tierIndex, long pos, long timestamp) {
            LOG.debug("raise change: id {}, tierIndex {}, pos {}",
                    localIdentifier, tierIndex, pos);
            long bitIndex = combine(tierIndex, pos);
            if (tierIndex <= actualSegments) {
                mainSegmentsModIterFrameForUpdates.set(nativeAccess(), null,
                        mainSegmentsChangesBitSetAddr, bitIndex);
            } else {
                raiseExtraTierChange(tierIndex, bitIndex);
            }
//            assert timestamp > timeProvider.currentTime() - TimeUnit.SECONDS.toMillis(1) &&
//                    timestamp <= timeProvider.currentTime() : "timeStamp=" + timestamp + ", " +
//                    "currentTime=" + timeProvider.currentTime();
            // todo improve this - use the timestamp from the entry its self
            bootStrapTimeStamp.compareAndSet(0, timestamp);
            if (modificationNotifier != null)
                modificationNotifier.onChange();
        }

        private void raiseExtraTierChange(long tierIndex, long bitIndex) {
            tierIndex = tierIndex - actualSegments - 1;
            long bulkIndex = tierIndex >> log2TiersInBulk;
            TierBulkData tierBulkData = tierBulkOffsets.get((int) bulkIndex);
            long bitSetAddr = tierBulkData.langBytes.address() + tierBulkData.offset +
                    offsetToBitSetWithinATierBulk;
            tierBulkModIterFrameForUpdates.set(nativeAccess(), null, bitSetAddr, bitIndex);
        }

        boolean dropChange(long tierIndex, long pos) {
            LOG.debug("drop change: id {}, tierIndex {}, pos {}", localIdentifier, tierIndex, pos);
            long bitIndex = combine(tierIndex, pos);
            if (tierIndex <= actualSegments) {
                return mainSegmentsModIterFrameForUpdates.clearIfSet(nativeAccess(), null,
                        mainSegmentsChangesBitSetAddr, bitIndex);
            } else {
                return dropExtraTierChange(tierIndex, bitIndex);
            }
        }

        private boolean dropExtraTierChange(long tierIndex, long bitIndex) {
            tierIndex = tierIndex - actualSegments - 1;
            long bulkIndex = tierIndex >> log2TiersInBulk;
            TierBulkData tierBulkData = tierBulkOffsets.get((int) bulkIndex);
            long bitSetAddr = tierBulkData.langBytes.address() + tierBulkData.offset +
                    offsetToBitSetWithinATierBulk;
            return tierBulkModIterFrameForUpdates.clearIfSet(nativeAccess(), null,
                    bitSetAddr, bitIndex);
        }

        boolean isChanged(long tierIndex, long pos) {
            long bitIndex = combine(tierIndex, pos);
            if (tierIndex <= actualSegments) {
                return mainSegmentsModIterFrameForUpdates.isSet(nativeAccess(), null,
                        mainSegmentsChangesBitSetAddr, bitIndex);
            } else {
                return isChangedExtraTier(tierIndex, bitIndex);
            }
        }

        private boolean isChangedExtraTier(long tierIndex, long bitIndex) {
            long extraTierIndex = tierIndex - actualSegments - 1;
            long bulkIndex = extraTierIndex >> log2TiersInBulk;
            TierBulkData tierBulkData = tierBulkOffsets.get((int) bulkIndex);
            long bitSetAddr = tierBulkData.langBytes.address() + tierBulkData.offset +
                    offsetToBitSetWithinATierBulk;
            return tierBulkModIterFrameForUpdates.isSet(nativeAccess(), null, bitSetAddr, bitIndex);
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
            return nextPosition() >= 0;
        }

        private long nextPosition() {
            long nextPos;
            long position = this.position;
            boolean allBitSetsScannedFromTheStart = false;
            // at most 2 iterations
            while (!allBitSetsScannedFromTheStart) {
                if (iterationMainSegmentsAreaOrTierBulk == -1) {
                    allBitSetsScannedFromTheStart = position < 0;
                    if ((nextPos = mainSegmentsModIterFrameForIteration.nextSetBit(nativeAccess(),
                            null, mainSegmentsChangesBitSetAddr, position + 1)) != NOT_FOUND) {
                        return nextPos;
                    } else {
                        this.position = position = NOT_FOUND;
                        // go to the first bulk
                        iterationMainSegmentsAreaOrTierBulk = 0;
                    }
                }
                // for each allocated tier bulk
                while (iterationMainSegmentsAreaOrTierBulk <
                        globalMutableState().getAllocatedExtraTierBulks()) {
                    VanillaChronicleHash.TierBulkData tierBulkData =
                            tierBulkOffsets.get(iterationMainSegmentsAreaOrTierBulk);
                    tierBulkBitSetAddr = tierBulkData.langBytes.address() + tierBulkData.offset +
                            offsetToBitSetWithinATierBulk;
                    if ((nextPos = tierBulkModIterFrameForIteration.nextSetBit(nativeAccess(), null,
                            tierBulkBitSetAddr, position + 1)) != NOT_FOUND) {
                        return nextPos;
                    }
                    // go to the next bulk
                    iterationMainSegmentsAreaOrTierBulk++;
                    this.position = position = NOT_FOUND;
                }
                this.iterationMainSegmentsAreaOrTierBulk = -1;
                this.position = position = NOT_FOUND;
                bootStrapTimeStamp.set(0);
            }
            return NOT_FOUND;
        }

        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        @Override
        public boolean nextEntry(@NotNull EntryCallback entryCallback, int chronicleId) {
            while (true) {
                long position = nextPosition();

                if (position == NOT_FOUND) {
                    this.position = NOT_FOUND;
                    return false;
                }

                this.position = position;
                int segmentIndexOrTierIndexOffsetWithinBulk =
                        (int) (position >>> segmentIndexShift);

                try (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R> context =
                        iterationContext()) {
                    if (iterationMainSegmentsAreaOrTierBulk < 0) {
                        // if main area
                        // segmentIndexOrTierIndexOffsetWithinBulk is segment index
                        context.initSegmentIndex(segmentIndexOrTierIndexOffsetWithinBulk);
                    } else {
                        // extra tiers
                        // segmentIndexOrTierIndexOffsetWithinBulk is tier index offset within bulk
                        TierBulkData tierBulkData =
                                tierBulkOffsets.get(iterationMainSegmentsAreaOrTierBulk);
                        long tierBaseAddr = tierAddr(tierBulkData,
                                segmentIndexOrTierIndexOffsetWithinBulk);
                        long tierCountersAreaAddr = tierBaseAddr + segmentHashLookupOuterSize;
                        context.initSegmentIndex(
                                TierCountersArea.segmentIndex(tierCountersAreaAddr));
                        int tier = TierCountersArea.tier(tierCountersAreaAddr);
                        long tierIndex = actualSegments +
                                (iterationMainSegmentsAreaOrTierBulk << log2TiersInBulk) +
                                segmentIndexOrTierIndexOffsetWithinBulk + 1;
                        context.initSegmentTier(tier, tierIndex, tierBaseAddr);
                    }

                    context.updateLock().lock();

                    if (changesForUpdatesGet(position)) {

                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        context.readExistingEntry(segmentPos);

                        // if the entry should be ignored, we'll move the next entry
                        if (entryCallback.shouldBeIgnored(context, chronicleId)) {
                            changesForUpdatesClear(position);
                            continue;
                        }

                        // it may not be successful if the buffer can not be re-sized so we will
                        // process it later, by NOT clearing the changes.clear(position)
                        context.segmentBytes().limit(context.valueOffset() + context.valueSize());
                        context.segmentBytes().position(context.keySizeOffset());
                        boolean success = entryCallback.onEntry(
                                context.segmentBytes(), chronicleId, bootStrapTimeStamp());
                        entryCallback.onAfterEntry();

                        if (success)
                            changesForUpdatesClear(position);

                        return success;
                    }
                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in relocation()),
                    // go to pick up next (next iteration in while (true) loop)
                }
            }
        }

        private boolean changesForUpdatesGet(long position) {
            if (iterationMainSegmentsAreaOrTierBulk < 0) {
                return mainSegmentsModIterFrameForUpdates.get(nativeAccess(), null,
                        mainSegmentsChangesBitSetAddr, position);
            } else {
                return tierBulkModIterFrameForUpdates.get(nativeAccess(), null,
                        tierBulkBitSetAddr, position);
            }
        }

        private void changesForUpdatesClear(long position) {
            if (iterationMainSegmentsAreaOrTierBulk < 0) {
                mainSegmentsModIterFrameForUpdates.clear(nativeAccess(), null,
                        mainSegmentsChangesBitSetAddr, position);
            } else {
                tierBulkModIterFrameForUpdates.clear(nativeAccess(), null,
                        tierBulkBitSetAddr, position);
            }
        }

        /**
         * @return the timestamp  that the remote client should bootstrap from when there has been a
         * disconnection, this time maybe later than the message time as event are not send in
         * chronological order from the bit set.
         */
        private long bootStrapTimeStamp() {
            final long timeStamp = bootStrapTimeStamp.get();
            long result = (timeStamp == 0) ? this.lastBootStrapTimeStamp : timeStamp;
            this.lastBootStrapTimeStamp = result;
            return result;
        }

        @Override
        public void dirtyEntries(long fromTimeStamp) {
            try (CompiledReplicatedMapIterationContext<K, KI, MKI, V, VI, MVI, R> c =
                         iterationContext()) {
                // iterate over all the segments and mark bit in the modification iterator
                // that correspond to entries with an older timestamp
                boolean debugEnabled = LOG.isDebugEnabled();
                for (int segmentIndex = 0; segmentIndex < actualSegments; segmentIndex++) {
                    c.initSegmentIndex(segmentIndex);
                    c.forEachSegmentReplicableEntry(e -> {
                        if (debugEnabled) {
                            LOG.debug("Bootstrap entry: id {}, key {}, value {}", localIdentifier,
                                    c.key(), c.value());
                        }
                        // Bizarrely the next line line cause NPE in JDT compiler
                        //assert re.originTimestamp() > 0L;
                        if (debugEnabled) {
                            LOG.debug("Bootstrap decision: bs ts: {}, entry ts: {}, " +
                                            "entry id: {}, local id: {}",
                                    fromTimeStamp, e.originTimestamp(),
                                    e.originIdentifier(), localIdentifier);
                        }
                        if (e.originTimestamp() >= fromTimeStamp &&
                                (!bootstrapOnlyLocalEntries ||
                                        e.originIdentifier() == localIdentifier)) {
                            raiseChange(c.tierIndex(), c.pos(), c.timestamp());
                        }
                    });
                }
            }
        }
    }
}