/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.bitset.BitSetFrame;
import net.openhft.chronicle.algo.bitset.SingleThreadedFlatBitSetFrame;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.hash.impl.TierCountersArea;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.impl.CompiledReplicatedMapIterationContext;
import net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import static net.openhft.chronicle.algo.MemoryUnit.*;
import static net.openhft.chronicle.algo.bitset.BitSetFrame.NOT_FOUND;
import static net.openhft.chronicle.algo.bytes.Access.nativeAccess;
import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;

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
 * reconciliation will ensure the maps themselves become eventually consistent. </p>
 *
 * <h2>Reconciliation </h2>
 * <p>If two ( or more nodes ) were to receive a change to their maps for
 * the same key but different values, say by a user of the maps, calling the put(key, value). Then,
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
 * node with the smallest identifier wins. </p>
 * <p>This a one of the basic building blocks needed to implement a fully-functioning Chronicle Map
 * cluster, such as that provided in
 * <a href="http://chronicle.software/products/chronicle-map/">Chronicle Map Enterprise</a>.</p>
 *
 * @param <K> the entries key type
 * @param <V> the entries value type
 */
public class ReplicatedChronicleMap<K, V, R> extends VanillaChronicleMap<K, V, R>
        implements Replica, Replica.EntryExternalizable {

    public static final int ADDITIONAL_ENTRY_BYTES = 10;
    static final byte ENTRY_HUNK = 1;
    static final byte BOOTSTRAP_TIME_HUNK = 2;

    public transient boolean cleanupRemovedEntries;
    public transient long cleanupTimeout;
    public transient TimeUnit cleanupTimeoutUnit;
    public transient MapRemoteOperations<K, V, R> remoteOperations;
    transient BitSetFrame tierModIterFrame;
    private long tierModIterBitSetSizeInBits;
    private long tierModIterBitSetOuterSize;
    private long segmentModIterBitSetsForIdentifierOuterSize;
    private long tierBulkModIterBitSetsForIdentifierOuterSize;
    private AtomicLong changeCount;
    private String globalMutableStateClass;

    /**
     * Default value is 0, that corresponds to "unset" identifier value (valid ids are positive)
     */
    private transient byte localIdentifier;
    /**
     * Idiomatically {@code assignedModificationIterators} should be a {@link CopyOnWriteArraySet},
     * but we should frequently iterate over this array without creating any garbage,
     * that is impossible with {@code CopyOnWriteArraySet}.
     */
    private transient ModificationIterator[] assignedModificationIterators;
    private transient AtomicReferenceArray<ModificationIterator> modificationIterators;
    private transient long startOfModificationIterators;
    private transient long[] remoteNodeCouldBootstrapFrom;

    public ReplicatedChronicleMap(@NotNull final ChronicleMapBuilder<K, V> builder) throws IOException {
        super(builder);
        tierModIterBitSetSizeInBits = computeTierModIterBitSetSizeInBits();
        tierModIterBitSetOuterSize = computeTierModIterBitSetOuterSize();
        segmentModIterBitSetsForIdentifierOuterSize =
                computeSegmentModIterBitSetsForIdentifierOuterSize();
        tierBulkModIterBitSetsForIdentifierOuterSize =
                computeTierBulkModIterBitSetsForIdentifierOuterSize(tiersInBulk);

        changeCount = new AtomicLong(0);
    }

    @Override
    protected void readMarshallableFields(@NotNull final WireIn wireIn) {
        super.readMarshallableFields(wireIn);

        tierModIterBitSetSizeInBits = wireIn.read("tierModIterBitSetSizeInBits").int64();
        tierModIterBitSetOuterSize = wireIn.read("tierModIterBitSetOuterSize").int64();
        segmentModIterBitSetsForIdentifierOuterSize =
                wireIn.read("segmentModIterBitSetsForIdentifierOuterSize").int64();
        tierBulkModIterBitSetsForIdentifierOuterSize =
                wireIn.read("tierBulkModIterBitSetsForIdentifierOuterSize").int64();

        globalMutableStateClass = wireIn.read("globalMutableStateClass").text();
        if (globalMutableStateClass == null) {
            // Missing "globalMutableStateClass" means we are reading from old data store file
            throw new UnsupportedOperationException("ReplicatedGlobalMutableState is no longer supported. Use a pre 3.22 version.");
            // Using legacy global mutable state class no longer supported
        }

        changeCount = new AtomicLong(0);
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wireOut) {
        super.writeMarshallable(wireOut);

        wireOut.write("tierModIterBitSetSizeInBits").int64(tierModIterBitSetSizeInBits);
        wireOut.write("tierModIterBitSetOuterSize").int64(tierModIterBitSetOuterSize);
        wireOut.write("segmentModIterBitSetsForIdentifierOuterSize")
                .int64(segmentModIterBitSetsForIdentifierOuterSize);
        wireOut.write("tierBulkModIterBitSetsForIdentifierOuterSize")
                .int64(tierBulkModIterBitSetsForIdentifierOuterSize);
        wireOut.write("globalMutableStateClass").text(globalMutableStateClass);
    }

    @Override
    protected VanillaGlobalMutableState createGlobalMutableState() {
        if (createdOrInMemory) {
            final Class<ReplicatedGlobalMutableStateV2> defaultClass = ReplicatedGlobalMutableStateV2.class;

            globalMutableStateClass = defaultClass.getName();
            // Must be set before writeHeader() call - classname should be persisted as a part of the header

            return Values.newNativeReference(defaultClass);
        }

        try {
            return (VanillaGlobalMutableState) Values.newNativeReference(Class.forName(globalMutableStateClass));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to resolve global mutable state class: " + globalMutableStateClass, e);
        }
    }

    @Override
    public ReplicatedGlobalMutableStateV2 globalMutableState() {
        throwExceptionIfClosed();

        return (ReplicatedGlobalMutableStateV2) super.globalMutableState();
    }

    @Override
    public void initTransients() {
        throwExceptionIfClosed();

        super.initTransients();
        initOwnTransients();
    }

    private void initOwnTransients() {
        //noinspection unchecked
        assignedModificationIterators = new ReplicatedChronicleMap.ModificationIterator[0];
        modificationIterators = new AtomicReferenceArray<>(128);
        tierModIterFrame = new SingleThreadedFlatBitSetFrame(computeTierModIterBitSetSizeInBits());
        remoteNodeCouldBootstrapFrom = new long[128];
    }

    @Override
    void initTransientsFromBuilder(@NotNull final ChronicleMapBuilder<K, V> builder) {
        super.initTransientsFromBuilder(builder);
        this.localIdentifier = builder.replicationIdentifier;
        //noinspection unchecked
        this.remoteOperations = (MapRemoteOperations<K, V, R>) builder.remoteOperations;
        cleanupRemovedEntries = builder.cleanupRemovedEntries;
        cleanupTimeout = builder.cleanupTimeout;
        cleanupTimeoutUnit = builder.cleanupTimeoutUnit;
    }

    private long computeTierModIterBitSetSizeInBits() {
        return LONGS.align(actualChunksPerSegmentTier, BITS);
    }

    private long computeTierModIterBitSetOuterSize() {
        long tierModIterBitSetOuterSize = BYTES.convert(computeTierModIterBitSetSizeInBits(), BITS);
        // protect from false sharing between bit sets of adjacent segments
        tierModIterBitSetOuterSize += BYTES.convert(2, CACHE_LINES);
        if (CACHE_LINES.align(tierModIterBitSetOuterSize, BYTES) == tierModIterBitSetOuterSize) {
            tierModIterBitSetOuterSize =
                    breakL1CacheAssociativityContention(tierModIterBitSetOuterSize);
        }
        return tierModIterBitSetOuterSize;
    }

    private long computeSegmentModIterBitSetsForIdentifierOuterSize() {
        return computeTierModIterBitSetOuterSize() * actualSegments;
    }

    private long computeTierBulkModIterBitSetsForIdentifierOuterSize(final long tiersInBulk) {
        return computeTierModIterBitSetOuterSize() * tiersInBulk;
    }

    @Override
    protected long computeTierBulkInnerOffsetToTiers(final long tiersInBulk) {
        long tierBulkBitSetsInnerSize =
                computeTierBulkModIterBitSetsForIdentifierOuterSize(tiersInBulk) * 128;
        return super.computeTierBulkInnerOffsetToTiers(tiersInBulk) +
                CACHE_LINES.align(tierBulkBitSetsInnerSize, BYTES);
    }

    @Override
    public long mapHeaderInnerSize() {
        throwExceptionIfClosed();

        return super.mapHeaderInnerSize() + (segmentModIterBitSetsForIdentifierOuterSize * 128);
    }

    @Override
    public void setRemoteNodeCouldBootstrapFrom(final byte remoteIdentifier, final long bootstrapTimestamp) {
        throwExceptionIfClosed();

        remoteNodeCouldBootstrapFrom[remoteIdentifier] = bootstrapTimestamp;
    }

    @Override
    public long remoteNodeCouldBootstrapFrom(final byte remoteIdentifier) {
        throwExceptionIfClosed();

        return remoteNodeCouldBootstrapFrom[remoteIdentifier];
    }

    @Override
    public void onHeaderCreated() {
        throwExceptionIfClosed();

        // Pad modification iterators at 3 cache lines from the end of the map header,
        // to avoid false sharing with the header of the first segment
        startOfModificationIterators = super.mapHeaderInnerSize() +
                RESERVED_GLOBAL_MUTABLE_STATE_BYTES - BYTES.convert(3, CACHE_LINES);
    }

    @Override
    protected void zeroOutNewlyMappedChronicleMapBytes() {
        super.zeroOutNewlyMappedChronicleMapBytes();
        bs.zeroOut(super.mapHeaderInnerSize(), this.mapHeaderInnerSize());
    }

    @Override
    public byte identifier() {
        throwExceptionIfClosed();

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
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        throwExceptionIfClosing();

        ModificationIterator modificationIterator = modificationIterators.get(remoteIdentifier);
        if (modificationIterator != null)
            return modificationIterator;

        globalMutableStateLock();
        try {
            modificationIterator = modificationIterators.get(remoteIdentifier);

            if (modificationIterator != null)
                return modificationIterator;

            final ReplicatedGlobalMutableStateV2 globalMutableState = globalMutableState();
            final boolean modificationIteratorInit =
                    globalMutableState.getModificationIteratorInitAt(remoteIdentifier);
            final ModificationIterator modIter =
                    new ModificationIterator(remoteIdentifier, modificationIteratorInit);
            if (!modificationIteratorInit) {
                globalMutableState.setModificationIteratorInitAt(remoteIdentifier, true);
                // This doesn't need to be volatile update, because modification iterators count
                // is checked (also non-volatile) in the beginning of raiseChange()/dropChange()
                // methods, the risk is that a new modification iterator is added, dirtyEntries()
                // already completed, and then we call raiseChange() and miss the new iterator.
                // raiseChange() is called, when the segment lock is held on update level, as well
                // as dirtyEntries() (that is segment iteration, which is always performed on
                // update-level lock), so the two actions (dirtyEntries() and raiseChange())
                // are serialized between each other, => change to ModificationIteratorsCount is
                // visible.
                globalMutableState.addModificationIteratorsCount(1);
            }
            //noinspection unchecked
            assignedModificationIterators = Stream
                    .concat(Arrays.stream(assignedModificationIterators), Stream.of(modIter))
                    .sorted(Comparator.comparing(it -> it.remoteIdentifier))
                    .toArray(ReplicatedChronicleMap.ModificationIterator[]::new);
            modificationIterators.set(remoteIdentifier, modIter);
            return modIter;
        } finally {
            globalMutableStateUnlock();
        }
    }

    public ModificationIterator[] acquireAllModificationIterators() {
        throwExceptionIfClosed();

        for (int remoteIdentifier = 0; remoteIdentifier < 128; remoteIdentifier++) {
            if (globalMutableState().getModificationIteratorInitAt(remoteIdentifier)) {
                acquireModificationIterator((byte) remoteIdentifier);
            }
        }
        return assignedModificationIterators;
    }

    private void updateModificationIteratorsArray() {
        if (globalMutableState().getModificationIteratorsCount() !=
                assignedModificationIterators.length) {
            acquireAllModificationIterators();
        }
    }

    public long changeCount() {
        throwExceptionIfClosed();

        return changeCount.get();
    }

    public void raiseChange(final long tierIndex, final long pos) {
        throwExceptionIfClosed();

        // -1 is invalid remoteIdentifier => raise change for all
        changeCount.incrementAndGet();
        raiseChangeForAllExcept(tierIndex, pos, (byte) -1);
    }

    public void raiseChangeFor(final long tierIndex,
                               final long pos,
                               final byte remoteIdentifier) {
        throwExceptionIfClosed();

        acquireModificationIterator(remoteIdentifier).raiseChange0(tierIndex, pos);
    }

    public void raiseChangeForAllExcept(final long tierIndex,
                                        final long pos,
                                        final byte remoteIdentifier) {
        throwExceptionIfClosed();

        updateModificationIteratorsArray();
        if (tierIndex <= actualSegments) {
            final long segmentIndex = tierIndex - 1;
            final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
            for (ModificationIterator it : assignedModificationIterators) {
                if (it.remoteIdentifier != remoteIdentifier)
                    it.raiseChangeInSegment(offsetToTierBitSet, pos);
            }
        } else {
            final long extraTierIndex = tierIndex - 1 - actualSegments;
            final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
            final long offsetToTierBitSet =
                    (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
            for (ModificationIterator it : assignedModificationIterators) {
                if (it.remoteIdentifier != remoteIdentifier)
                    it.raiseChangeInTierBulk(bulkIndex, offsetToTierBitSet, pos);
            }
        }
    }

    public void dropChange(final long tierIndex, final long pos) {
        throwExceptionIfClosed();

        updateModificationIteratorsArray();
        if (tierIndex <= actualSegments) {
            final long segmentIndex = tierIndex - 1;
            final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
            for (ModificationIterator modificationIterator : assignedModificationIterators) {
                modificationIterator.dropChangeInSegment(offsetToTierBitSet, pos);
            }
        } else {
            final long extraTierIndex = tierIndex - 1 - actualSegments;
            final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
            final long offsetToTierBitSet =
                    (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
            for (ModificationIterator modificationIterator : assignedModificationIterators) {
                modificationIterator.dropChangeInTierBulk(bulkIndex, offsetToTierBitSet, pos);
            }
        }
    }

    public void dropChangeFor(final long tierIndex,
                              final long pos,
                              final byte remoteIdentifier) {
        throwExceptionIfClosed();

        acquireModificationIterator(remoteIdentifier).dropChange0(tierIndex, pos);
    }

    public void moveChange(final long oldTierIndex,
                           final long oldPos,
                           final long newTierIndex,
                           final long newPos) {
        throwExceptionIfClosed();

        updateModificationIteratorsArray();
        if (oldTierIndex <= actualSegments) {
            final long oldSegmentIndex = oldTierIndex - 1;
            final long oldOffsetToTierBitSet = oldSegmentIndex * tierModIterBitSetOuterSize;
            if (newTierIndex <= actualSegments) {
                final long newSegmentIndex = newTierIndex - 1;
                final long newOffsetToTierBitSet = newSegmentIndex * tierModIterBitSetOuterSize;
                for (ModificationIterator modificationIterator : assignedModificationIterators) {
                    if (modificationIterator.dropChangeInSegment(oldOffsetToTierBitSet, oldPos))
                        modificationIterator.raiseChangeInSegment(newOffsetToTierBitSet, newPos);
                }
            } else {
                final long newExtraTierIndex = newTierIndex - 1 - actualSegments;
                final int newBulkIndex = (int) (newExtraTierIndex >> log2TiersInBulk);
                final long newOffsetToTierBitSet =
                        (newExtraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
                for (ModificationIterator modificationIterator : assignedModificationIterators) {
                    if (modificationIterator.dropChangeInSegment(oldOffsetToTierBitSet, oldPos)) {
                        modificationIterator
                                .raiseChangeInTierBulk(newBulkIndex, newOffsetToTierBitSet, newPos);
                    }
                }
            }
        } else {
            final long oldExtraTierIndex = oldTierIndex - 1 - actualSegments;
            final int oldBulkIndex = (int) (oldExtraTierIndex >> log2TiersInBulk);
            final long oldOffsetToTierBitSet =
                    (oldExtraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
            if (newTierIndex <= actualSegments) {
                final long newSegmentIndex = newTierIndex - 1;
                final long newOffsetToTierBitSet = newSegmentIndex * tierModIterBitSetOuterSize;
                for (ModificationIterator modificationIterator : assignedModificationIterators) {
                    if (modificationIterator
                            .dropChangeInTierBulk(oldBulkIndex, oldOffsetToTierBitSet, oldPos)) {
                        modificationIterator.raiseChangeInSegment(newOffsetToTierBitSet, newPos);
                    }
                }
            } else {
                final long newExtraTierIndex = newTierIndex - 1 - actualSegments;
                final int newBulkIndex = (int) (newExtraTierIndex >> log2TiersInBulk);
                final long newOffsetToTierBitSet =
                        (newExtraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
                for (ModificationIterator modificationIterator : assignedModificationIterators) {
                    if (modificationIterator
                            .dropChangeInTierBulk(oldBulkIndex, oldOffsetToTierBitSet, oldPos)) {
                        modificationIterator
                                .raiseChangeInTierBulk(newBulkIndex, newOffsetToTierBitSet, newPos);
                    }
                }
            }
        }
    }

    public boolean isChanged(final long tierIndex, final long pos) {
        throwExceptionIfClosed();

        updateModificationIteratorsArray();
        if (tierIndex <= actualSegments) {
            final long segmentIndex = tierIndex - 1;
            final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
            for (ModificationIterator modificationIterator : assignedModificationIterators) {
                if (modificationIterator.isChangedSegment(offsetToTierBitSet, pos))
                    return true;
            }
        } else {
            final long extraTierIndex = tierIndex - 1 - actualSegments;
            final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
            final long offsetToTierBitSet =
                    (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
            for (ModificationIterator modificationIterator : assignedModificationIterators) {
                if (modificationIterator.isChangedTierBulk(bulkIndex, offsetToTierBitSet, pos))
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean identifierCheck(@NotNull final ReplicableEntry entry, final int chronicleId) {
        throwExceptionIfClosed();

        return entry.originIdentifier() == identifier();
    }

    @Override
    public void writeExternalEntry(final ReplicableEntry entry,
                                   final Bytes payload,
                                   @NotNull final Bytes destination,
                                   final int chronicleId,
                                   final ArrayList<String> keys) {
        throwExceptionIfClosed();

        if (payload != null)
            writePayload(payload, destination);
        if (entry != null)
            writeExternalEntry0(entry, destination, keys);
    }

    private void writePayload(final Bytes payload, final Bytes destination) {
        destination.writeByte(BOOTSTRAP_TIME_HUNK);
        destination.write(payload, payload.readPosition(), payload.readRemaining());
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    private void writeExternalEntry0(final ReplicableEntry entry,
                                     final Bytes destination,
                                     final ArrayList<String> keys) {
        destination.writeByte(ENTRY_HUNK);

        destination.writeStopBit(entry.originTimestamp());
        if (entry.originIdentifier() == 0)
            throw new IllegalStateException("Identifier can't be 0");
        destination.writeByte(entry.originIdentifier());

        Data key;
        boolean isDeleted;
        if (entry instanceof MapEntry) {
            isDeleted = false;
            key = ((MapEntry) entry).key();
        } else {
            isDeleted = true;
            key = ((MapAbsentEntry) entry).absentKey();
        }

        try {
            keys.add(key.get().toString());
        } catch (Exception e) {
            keys.add("<Binary Data>");
        }

        destination.writeBoolean(isDeleted);

        keySizeMarshaller.writeSize(destination, key.size());
        key.writeTo(destination, destination.writePosition());
        destination.writeSkip(key.size());

        final boolean traceEnabled = Jvm.isDebugEnabled(getClass());
        String message = null;
        if (traceEnabled) {
            if (isDeleted) {
                Jvm.debug().on(getClass(),
                        "WRITING ENTRY TO DEST -  into local-id=identifier(), remove(key=" + key + ")");
            } else {
                message = String.format(
                        "WRITING ENTRY TO DEST  -  into local-id=%d, put(key=%s,",
                        identifier(), key);
            }
        }

        if (isDeleted)
            return;

        final Data value = ((MapEntry) entry).value();
        valueSizeMarshaller.writeSize(destination, value.size());
        value.writeTo(destination, destination.writePosition());
        destination.writeSkip(value.size());

        if (traceEnabled) {
            Jvm.debug().on(getClass(), message + "value=" + value + ")");
        }
    }

    @Override
    ChainingInterface newQueryContext() {
        return new CompiledReplicatedMapQueryContext<>(this);
    }

    @Override
    public CompiledReplicatedMapQueryContext<K, V, R> mapContext() {
        //noinspection unchecked
        return q().getContext(CompiledReplicatedMapQueryContext.class,
                // lambda is used instead of constructor reference because currently stage-compiler
                // has issues with parsing method/constructor refs.
                // TODO replace with constructor ref when stage-compiler is improved
                CompiledReplicatedMapQueryContext::new, this);
    }

    /**
     * This method does not set a segment lock, A segment lock should be obtained before calling
     * this method, especially when being used in a multi threaded context.
     */
    @Override
    public void readExternalEntry(@NotNull final Bytes source, final byte remoteNodeIdentifier) {
        throwExceptionIfClosed();

        byte hunk = source.readByte();
        if (hunk == BOOTSTRAP_TIME_HUNK) {
            setRemoteNodeCouldBootstrapFrom(remoteNodeIdentifier, source.readLong());
        } else {
            assert hunk == ENTRY_HUNK;
            try (CompiledReplicatedMapQueryContext<K, V, R> remoteOpContext = mapContext()) {
                remoteOpContext.processReplicatedEvent(remoteNodeIdentifier, source);
            }
        }
    }

    @Override
    ChainingInterface newIterationContext() {
        return new CompiledReplicatedMapIterationContext<>(this);
    }

    public CompiledReplicatedMapIterationContext<K, V, R> iterationContext() {
        //noinspection unchecked
        return i().getContext(CompiledReplicatedMapIterationContext.class,
                // lambda is used instead of constructor reference because currently stage-compiler
                // has issues with parsing method/constructor refs.
                // TODO replace with constructor ref when stage-compiler is improved
                CompiledReplicatedMapIterationContext::new, this);
    }

    @Override
    public final V get(final Object key) {
        return defaultGet(key);
    }

    @Override
    public final V getUsing(final K key, final V usingValue) {
        return defaultGetUsing(key, usingValue);
    }

    /**
     * <p>
     * Once a change occurs to a map, map replication requires that these changes are picked up
     * by another thread, this class provides an iterator like interface to poll for such changes.
     * </p>
     * <p>
     * In most cases the thread that adds data to the node is unlikely to be the same thread
     * that replicates the data over to the other nodes, so data will have to be marshaled between
     * the main thread storing data to the map, and the thread running the replication.
     * </p>
     * <p>
     * One way to perform this marshalling, would be to pipe the data into a queue. However, This class
     * takes another approach. It uses a bit set, and marks bits which correspond to the indexes of
     * the entries that have changed. It then provides an iterator like interface to poll for such
     * changes.
     * </p>
     *
     * @author Rob Austin.
     */
    public final class ModificationIterator implements Replica.ModificationIterator {
        private final byte remoteIdentifier;
        private final long segmentBitSetsAddr;
        private final long offsetToBitSetsWithinATierBulk;

        private ModificationNotifier modificationNotifier;

        private long bootstrapTimeAfterNextIterationComplete = 0L;
        private boolean somethingSentOnThisIteration = false;

        // The iteration "cursor" consists of 4 fields:
        // 1) if segmentIndex >= 0, bulkIndex = -1, tierIndexOffsetWithinBulk = -1:
        // => we are in "first tiers" aka "segments"
        // 2) segmentIndex = -1, bulkIndex >= 0, tierIndexOffsetWithinBulk >= 0:
        // => we are in extra tiers
        private int segmentIndex;
        private int bulkIndex;
        private int tierIndexOffsetWithinBulk;
        /**
         * Corresponds to {@link net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages#pos}
         */
        private long entryPos;

        /**
         * Cached addr of the changes bit set of the current tier (which "cursor" points to),
         * to avoid re-computation of this addr in {@link #entryIsStillDirty(long)} and
         * {@link #clearEntry(long)} methods
         */
        private long tierBitSetAddr;

        public ModificationIterator(final byte remoteIdentifier, final boolean sharedMemoryInit) {
            this.remoteIdentifier = remoteIdentifier;
            segmentBitSetsAddr = bsAddress() + startOfModificationIterators +
                    remoteIdentifier * segmentModIterBitSetsForIdentifierOuterSize;
            if (!sharedMemoryInit) {
                nativeAccess().zeroOut(null, segmentBitSetsAddr,
                        segmentModIterBitSetsForIdentifierOuterSize);
            }
            offsetToBitSetsWithinATierBulk =
                    remoteIdentifier * tierBulkModIterBitSetsForIdentifierOuterSize;

            resetCursor();
        }

        private void resetCursor() {
            segmentIndex = 0;
            bulkIndex = -1;
            tierIndexOffsetWithinBulk = -1;
            entryPos = -1;

            tierBitSetAddr = segmentBitSetsAddr; // + tierModIterBitSetOuterSize * segmentIndex = 0
        }

        public void setModificationNotifier(@NotNull final ModificationNotifier modificationNotifier) {
            throwExceptionIfClosed();

            this.modificationNotifier = modificationNotifier;
        }

        void raiseChangeInSegment(final long offsetToTierBitSet, final long pos) {
            tierModIterFrame.set(nativeAccess(), null,
                    segmentBitSetsAddr + offsetToTierBitSet, pos);
            if (modificationNotifier != null)
                modificationNotifier.onChange();
        }

        void raiseChangeInTierBulk(final int bulkIndex,
                                   final long offsetToTierBitSet,
                                   final long pos) {
            final TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
            final long bitSetAddr = bitSetsAddr(tierBulkData) + offsetToTierBitSet;
            tierModIterFrame.set(nativeAccess(), null, bitSetAddr, pos);
            if (modificationNotifier != null)
                modificationNotifier.onChange();
        }

        boolean dropChangeInSegment(final long offsetToTierBitSet,
                                    final long pos) {
            return tierModIterFrame.clearIfSet(nativeAccess(), null,
                    segmentBitSetsAddr + offsetToTierBitSet, pos);
        }

        boolean dropChangeInTierBulk(final int bulkIndex,
                                     final long offsetToTierBitSet,
                                     final long pos) {
            final TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
            final long bitSetAddr = bitSetsAddr(tierBulkData) + offsetToTierBitSet;
            return tierModIterFrame.clearIfSet(nativeAccess(), null, bitSetAddr, pos);
        }

        boolean isChangedSegment(final long offsetToTierBitSet, final long pos) {
            return tierModIterFrame.isSet(nativeAccess(), null,
                    segmentBitSetsAddr + offsetToTierBitSet, pos);
        }

        boolean isChangedTierBulk(final int bulkIndex,
                                  final long offsetToTierBitSet,
                                  final long pos) {
            final TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
            final long bitSetAddr = bitSetsAddr(tierBulkData) + offsetToTierBitSet;
            return tierModIterFrame.isSet(nativeAccess(), null, bitSetAddr, pos);
        }

        private long bitSetsAddr(final TierBulkData tierBulkData) {
            return tierBulkData.bytesStore.addressForRead(tierBulkData.offset) +
                    offsetToBitSetsWithinATierBulk;
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
            throwExceptionIfClosed();

            return nextEntryPos(null, 0) != NOT_FOUND;
        }

        private long nextEntryPos(final Callback callback, final int chronicleId) {
            long nextEntryPos;
            boolean allBitSetsScannedFromTheStart = false;
            // at most 2 iterations
            while (!allBitSetsScannedFromTheStart) {
                if (segmentIndex >= 0) {
                    allBitSetsScannedFromTheStart = segmentIndex == 0 && entryPos == -1;
                    if (allBitSetsScannedFromTheStart) {
                        bootstrapTimeAfterNextIterationComplete = currentTime();
                        somethingSentOnThisIteration = false;
                    }

                    while (segmentIndex < actualSegments) {
                        // This is needed to ensure, that any entry update with the timestamp,
                        // smaller than assigned for bootstrapTimeAfterNextIterationComplete, is
                        // visible during the iteration of the current segment. Bits are raised
                        // during the update via non-volatile bit set (performance concerns), hence
                        // to guarantee visibility during the iteration, we build a happens-before
                        // between bit raise and bit reading:
                        // bit raised ->
                        // lock released (end of update operation) ->
                        // lock acquired (the following acquireAndReleaseUpdateLock() call) ->
                        // bits are iterated (raised bits should be visible here)
                        if (entryPos == -1) {
                            acquireAndReleaseUpdateLock(segmentIndex);
                        }

                        if ((nextEntryPos = tierModIterFrame.nextSetBit(nativeAccess(),
                                null, tierBitSetAddr, entryPos + 1)) != NOT_FOUND) {
                            return nextEntryPos;
                        } else {
                            segmentIndex++;
                            tierBitSetAddr += tierModIterBitSetOuterSize;
                            entryPos = -1;
                        }
                    }
                    // go to extra bulks
                    segmentIndex = -1;
                    bulkIndex = 0;
                    tierIndexOffsetWithinBulk = 0;
                    if (bulkIndex < globalMutableState().getAllocatedExtraTierBulks())
                        tierBitSetAddr = bitSetsAddr(tierBulkOffsets.get(bulkIndex));
                }
                // for each allocated tier bulk
                while (bulkIndex < globalMutableState().getAllocatedExtraTierBulks()) {
                    while (tierIndexOffsetWithinBulk < tiersInBulk) {
                        if ((nextEntryPos = tierModIterFrame.nextSetBit(nativeAccess(), null,
                                tierBitSetAddr, entryPos + 1)) != NOT_FOUND) {
                            return nextEntryPos;
                        } else {
                            tierIndexOffsetWithinBulk++;
                            tierBitSetAddr += tierModIterBitSetOuterSize;
                            entryPos = -1;
                        }
                    }
                    // go to the next bulk
                    bulkIndex++;
                    tierIndexOffsetWithinBulk = 0;
                    if (bulkIndex < globalMutableState().getAllocatedExtraTierBulks())
                        tierBitSetAddr = bitSetsAddr(tierBulkOffsets.get(bulkIndex));
                }

                resetCursor();

                // we walked through the whole chronicle map instance, "iteration"
                if (callback != null && somethingSentOnThisIteration) {
                    callback.onBootstrapTime(bootstrapTimeAfterNextIterationComplete, chronicleId);
                }
            }
            return NOT_FOUND;
        }

        private void acquireAndReleaseUpdateLock(final int segmentIndex) {
            try (CompiledReplicatedMapIterationContext<K, V, R> c = iterationContext()) {
                c.initSegmentIndex(segmentIndex);
                c.updateLock().lock();
            }
        }

        /**
         * @param callback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        @Override
        public boolean nextEntry(@NotNull final Callback callback, final int chronicleId) {
            throwExceptionIfClosed();

            while (true) {
                try (CompiledReplicatedMapIterationContext<K, V, R> context = iterationContext()) {
                    final long nextEntryPos = nextEntryPos(callback, chronicleId);
                    if (nextEntryPos == NOT_FOUND)
                        return false;
                    entryPos = nextEntryPos;

                    if (segmentIndex >= 0) {
                        // we are in first tiers (aka "segments")
                        context.initSegmentIndex(segmentIndex);
                    } else {
                        // we are in extra tiers
                        final TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
                        final long tierBaseAddr = tierAddr(tierBulkData, tierIndexOffsetWithinBulk);
                        final long tierCountersAreaAddr = tierBaseAddr + tierHashLookupOuterSize;
                        context.initSegmentIndex(
                                TierCountersArea.segmentIndex(tierCountersAreaAddr));
                        final int tier = TierCountersArea.tier(tierCountersAreaAddr);
                        final long tierIndex = actualSegments +
                                ((long) bulkIndex << log2TiersInBulk) + tierIndexOffsetWithinBulk + 1;
                        context.initSegmentTier(tier, tierIndex, tierBaseAddr);
                    }

                    context.updateLock().lock();

                    if (entryIsStillDirty(entryPos)) {
                        context.readExistingEntry(entryPos);
                        final ReplicableEntry entry = (ReplicableEntry) context.entryForIteration();
                        callback.onEntry(entry, chronicleId);
                        somethingSentOnThisIteration = true;
                        clearEntry(entryPos);
                        return true;
                    }
                    // if the entryPos was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in relocation()),
                    // go to pick up next (next iteration in the `while (true)` loop)
                }
            }
        }

        private boolean entryIsStillDirty(final long entryPos) {
            return tierModIterFrame.get(nativeAccess(), null, tierBitSetAddr, entryPos);
        }

        private void clearEntry(final long entryPos) {
            tierModIterFrame.clear(nativeAccess(), null, tierBitSetAddr, entryPos);
        }

        /**
         * Handler that performs background invalidation of possibly outdated entries on node restart.
         * Outgoing replication is not blocked by this process - invalidated entries will be eventually picked
         * by the modification iterator.
         */
        class DirtyEntriesHandler implements EventHandler {
            private final IntPredicate segmentPredicate;
            private final long fromTimeStamp;
            private int segmentIndex;
            private CompiledReplicatedMapIterationContext<K, V, R> iterationContext;

            @Override
            public @NotNull
            HandlerPriority priority() {
                return HandlerPriority.CONCURRENT;
            }

            public DirtyEntriesHandler(IntPredicate segmentPredicate, long fromTimeStamp) {
                this.segmentPredicate = segmentPredicate;
                this.fromTimeStamp = fromTimeStamp;
            }

            @Override
            public boolean action() throws InvalidEventHandlerException {
                if (segmentIndex >= actualSegments) {
                    if (iterationContext != null)
                        iterationContext.close();

                    throw new InvalidEventHandlerException("background work is done, removing the handler");
                }

                if (!segmentPredicate.test(segmentIndex)) {
                    segmentIndex++;

                    return true;
                }

                final boolean debugEnabled = Jvm.isDebugEnabled(getClass());

                try {
                    throwExceptionIfClosed();

                    if (iterationContext == null)
                        iterationContext = iterationContext();

                    iterationContext.initSegmentIndex(segmentIndex);
                    iterationContext.forEachSegmentReplicableEntry(e -> {
                        if (debugEnabled) {
                            Jvm.debug().on(getClass(), "Bootstrap entry: " +
                                    "id " + localIdentifier + ", " +
                                    "key " + iterationContext.key() + ", " +
                                    "value " + iterationContext.value());
                        }
                        // Bizarrely the next line line cause NPE in JDT compiler
                        //assert re.originTimestamp() > 0L;
                        if (debugEnabled) {
                            Jvm.debug().on(getClass(), "Bootstrap decision: " +
                                    "bs ts: " + fromTimeStamp + ", " +
                                    "entry ts: " + e.originTimestamp() + ", " +
                                    "entry id: " + e.originIdentifier() + ", " +
                                    "local id: " + localIdentifier);
                        }
                        // TODO currently, all entries, originating not from the current node,
                        // are bootstrapped. This could be optimized, but requires to generate
                        // unique connection id, it identify two ChronicleMap instances
                        // reconnecting vs. different Map start-up
                        if (e.originIdentifier() != localIdentifier ||
                                e.originTimestamp() >= fromTimeStamp) {
                            raiseChange0(iterationContext.tierIndex(), iterationContext.pos());
                        }
                    });
                } catch (ChronicleHashClosedException e) {
                    if (iterationContext != null)
                        iterationContext.close();

                    throw new InvalidEventHandlerException(e);
                }

                segmentIndex++;

                return true;
            }

        }

        @Override
        public void dirtyEntries(long fromTimeStamp, @NotNull EventLoop eventLoop) {
            throwExceptionIfClosed();

            for (int i = 0; i < EventGroup.CONC_THREADS; i++) {
                final int finalI = i;
                eventLoop.addHandler(new DirtyEntriesHandler(
                        segment -> segment % EventGroup.CONC_THREADS == finalI, fromTimeStamp));
            }
        }

        void raiseChange0(final long tierIndex, final long pos) {
            if (tierIndex <= actualSegments) {
                final long segmentIndex = tierIndex - 1;
                final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
                raiseChangeInSegment(offsetToTierBitSet, pos);
            } else {
                final long extraTierIndex = tierIndex - 1 - actualSegments;
                final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
                final long offsetToTierBitSet =
                        (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
                raiseChangeInTierBulk(bulkIndex, offsetToTierBitSet, pos);
            }
        }

        void dropChange0(final long tierIndex, final long pos) {
            if (tierIndex <= actualSegments) {
                final long segmentIndex = tierIndex - 1;
                final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
                dropChangeInSegment(offsetToTierBitSet, pos);
            } else {
                final long extraTierIndex = tierIndex - 1 - actualSegments;
                final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
                final long offsetToTierBitSet =
                        (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
                dropChangeInTierBulk(bulkIndex, offsetToTierBitSet, pos);
            }
        }

        public void clearRange0(final long tierIndex,
                                final long pos,
                                final long endPosExclusive) {
            throwExceptionIfClosed();

            if (tierIndex <= actualSegments) {
                final long segmentIndex = tierIndex - 1;
                final long offsetToTierBitSet = segmentIndex * tierModIterBitSetOuterSize;
                tierModIterFrame.clearRange(nativeAccess(), null,
                        segmentBitSetsAddr + offsetToTierBitSet, pos, endPosExclusive);
            } else {
                final long extraTierIndex = tierIndex - 1 - actualSegments;
                final int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
                final long offsetToTierBitSet =
                        (extraTierIndex & (tiersInBulk - 1)) * tierModIterBitSetOuterSize;
                final TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
                final long bitSetAddr = bitSetsAddr(tierBulkData) + offsetToTierBitSet;
                tierModIterFrame.clearRange(nativeAccess(), null, bitSetAddr, pos, endPosExclusive);
            }
        }
    }
}