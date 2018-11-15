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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.algo.locks.*;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytesStoreFactory;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.impl.util.Cleaner;
import net.openhft.chronicle.hash.impl.util.CleanerUtils;
import net.openhft.chronicle.hash.impl.util.jna.PosixFallocate;
import net.openhft.chronicle.hash.impl.util.jna.PosixMsync;
import net.openhft.chronicle.hash.impl.util.jna.WindowsMsync;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.ChronicleHashCorruptionImpl;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static net.openhft.chronicle.algo.MemoryUnit.*;
import static net.openhft.chronicle.algo.bytes.Access.nativeAccess;
import static net.openhft.chronicle.core.OS.pageAlign;
import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.*;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.format;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.report;

public abstract class VanillaChronicleHash<K,
        C extends HashEntry<K>, SC extends HashSegmentContext<K, ?>,
        ECQ extends ExternalHashQueryContext<K>>
        implements ChronicleHash<K, C, SC, ECQ>, Marshallable {

    public static final long TIER_COUNTERS_AREA_SIZE = 64;
    public static final long RESERVED_GLOBAL_MUTABLE_STATE_BYTES = 1024;

    // --- Start of instance fields ---
    /**
     * Global mutable state lock doesn't yet need read-write levels and waits;
     * Used the same locking strategy as in segment locks
     * (VanillaReadWriteUpdateWithWaitsLockingStrategy) in order to simplify Chronicle Map
     * specification (having only one kind of locks to specify and implement).
     */
    static final LockingStrategy globalMutableStateLockingStrategy =
            VanillaReadWriteUpdateWithWaitsLockingStrategy.instance();
    static final TryAcquireOperation<LockingStrategy> globalMutableStateLockTryAcquireOperation =
            TryAcquireOperations.lock();
    static final
    AcquisitionStrategy<LockingStrategy, RuntimeException>
            globalMutableStateLockAcquisitionStrategy =
            AcquisitionStrategies.spinLoopOrFail(2, TimeUnit.SECONDS);
    private static final long GLOBAL_MUTABLE_STATE_LOCK_OFFSET = 0L;
    private static final long GLOBAL_MUTABLE_STATE_VALUE_OFFSET = 8L;
    /////////////////////////////////////////////////
    // If the hash was created in the first place, or read from disk
    public transient boolean createdOrInMemory;
    /////////////////////////////////////////////////
    // Key Data model
    public Class<K> keyClass;
    public SizeMarshaller keySizeMarshaller;
    public SizedReader<K> keyReader;
    public DataAccess<K> keyDataAccess;
    /////////////////////////////////////////////////
    public boolean checksumEntries;
    /////////////////////////////////////////////////
    // Concurrency (number of segments), memory management and dependent fields
    public int actualSegments;
    public HashSplitting hashSplitting;
    public long chunkSize;
    public int maxChunksPerEntry;
    public long actualChunksPerSegmentTier;
    public int tierHashLookupValueBits;
    public int tierHashLookupKeyBits;
    public int tierHashLookupSlotSize;
    public long tierHashLookupCapacity;
    public long maxEntriesPerHashLookup;
    public long tierHashLookupOuterSize;
    public long tierFreeListInnerSize;
    public long tierFreeListOuterSize;
    public int tierEntrySpaceInnerOffset;
    public long tierSize;
    public long tiersInBulk;
    public transient List<TierBulkData> tierBulkOffsets;
    public transient long headerSize;
    public transient long segmentHeadersOffset;
    /////////////////////////////////////////////////
    // Miscellaneous fields
    public transient CompactOffHeapLinearHashTable hashLookup;
    public transient Identity identity;
    protected int log2TiersInBulk;
    private Runnable preShutdownAction;
    /////////////////////////////////////////////////
    // Bytes Store (essentially, the base address) and serialization-dependent offsets
    protected transient BytesStore bs;
    /////////////////////////////////////////////////
    // Precomputed offsets and sizes for fast Context init
    int segmentHeaderSize;
    long tierHashLookupInnerSize;
    long tierEntrySpaceInnerSize;
    long tierEntrySpaceOuterSize;
    long maxExtraTiers;
    long tierBulkSizeInBytes;
    long tierBulkInnerOffsetToTiers;
    transient long segmentsOffset;
    /////////////////////////////////////////////////
    private String dataFileVersion;
    /////////////////////////////////////////////////
    // Resources
    private transient File file;
    private transient RandomAccessFile raf;

    // --- End of instance fields ---
    private transient ChronicleHashResources resources;
    private transient Cleaner cleaner;
    private transient VanillaGlobalMutableState globalMutableState;

    public VanillaChronicleHash(ChronicleMapBuilder<K, ?> builder) {
        // Version
        dataFileVersion = BuildVersion.version();

        createdOrInMemory = true;

        @SuppressWarnings({"deprecation", "unchecked"})
        ChronicleHashBuilderPrivateAPI<K, ?> privateAPI =
                (ChronicleHashBuilderPrivateAPI<K, ?>) builder.privateAPI();

        // Data model
        SerializationBuilder<K> keyBuilder = privateAPI.keyBuilder();
        keyClass = keyBuilder.tClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        keyReader = keyBuilder.reader();
        keyDataAccess = keyBuilder.dataAccess();

        actualSegments = privateAPI.actualSegments();
        hashSplitting = HashSplitting.forSegments(actualSegments);

        chunkSize = privateAPI.chunkSize();
        maxChunksPerEntry = privateAPI.maxChunksPerEntry();
        actualChunksPerSegmentTier = privateAPI.actualChunksPerSegmentTier();

        // Precomputed offsets and sizes for fast Context init
        segmentHeaderSize = privateAPI.segmentHeaderSize();

        tierHashLookupValueBits = valueBits(actualChunksPerSegmentTier);
        tierHashLookupKeyBits = keyBits(privateAPI.entriesPerSegment(), tierHashLookupValueBits);
        tierHashLookupSlotSize =
                entrySize(tierHashLookupKeyBits, tierHashLookupValueBits);
        if (!privateAPI.aligned64BitMemoryOperationsAtomic() && tierHashLookupSlotSize > 4) {
            throw new IllegalStateException("aligned64BitMemoryOperationsAtomic() == false, " +
                    "but hash lookup slot is " + tierHashLookupSlotSize);
        }
        tierHashLookupCapacity = privateAPI.tierHashLookupCapacity();
        maxEntriesPerHashLookup = (long) (tierHashLookupCapacity * MAX_LOAD_FACTOR);
        tierHashLookupInnerSize = tierHashLookupCapacity * tierHashLookupSlotSize;
        tierHashLookupOuterSize = CACHE_LINES.align(tierHashLookupInnerSize, BYTES);

        tierFreeListInnerSize = LONGS.align(
                BYTES.alignAndConvert(actualChunksPerSegmentTier, BITS), BYTES);
        tierFreeListOuterSize = CACHE_LINES.align(tierFreeListInnerSize, BYTES);

        tierEntrySpaceInnerSize = chunkSize * actualChunksPerSegmentTier;
        tierEntrySpaceInnerOffset = privateAPI.segmentEntrySpaceInnerOffset();
        tierEntrySpaceOuterSize = CACHE_LINES.align(
                tierEntrySpaceInnerOffset + tierEntrySpaceInnerSize, BYTES);

        tierSize = tierSize();

        maxExtraTiers = privateAPI.maxExtraTiers();
        tiersInBulk = computeNumberOfTiersInBulk();
        log2TiersInBulk = Maths.intLog2(tiersInBulk);
        tierBulkInnerOffsetToTiers = computeTierBulkInnerOffsetToTiers(tiersInBulk);
        tierBulkSizeInBytes = computeTierBulkBytesSize(tiersInBulk);

        checksumEntries = privateAPI.checksumEntries();

        preShutdownAction = privateAPI.getPreShutdownAction();
    }

    public static IOException throwRecoveryOrReturnIOException(
            File file, String message, boolean recover) {
        message = "file=" + file + " " + message;
        if (recover) {
            throw new ChronicleHashRecoveryFailedException(message);
        } else {
            return new IOException(message);
        }
    }

    private static long roundUpMapHeaderSize(long headerSize) {
        return CACHE_LINES.align(headerSize, BYTES);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) {
        readMarshallableFields(wire);
        initTransients();
    }

    public Runnable getPreShutdownAction() {
        return preShutdownAction;
    }

    protected void readMarshallableFields(@NotNull WireIn wireIn) {
        dataFileVersion = wireIn.read(() -> "dataFileVersion").text();

        // Previously this assignment was done in default field initializer, but with Wire
        // serialization VanillaChronicleMap instance is created with unsafe.allocateInstance(),
        // that doesn't guarantee (?) to initialize fields with default values (false for boolean)
        createdOrInMemory = false;

        keyClass = wireIn.read(() -> "keyClass").typeLiteral();
        keySizeMarshaller = wireIn.read(() -> "keySizeMarshaller").object(SizeMarshaller.class);
        keyReader = wireIn.read(() -> "keyReader").object(SizedReader.class);
        keyDataAccess = wireIn.read(() -> "keyDataAccess").object(DataAccess.class);

        checksumEntries = wireIn.read(() -> "checksumEntries").bool();

        actualSegments = wireIn.read(() -> "actualSegments").int32();
        hashSplitting = wireIn.read(() -> "hashSplitting").typedMarshallable();

        chunkSize = wireIn.read(() -> "chunkSize").int64();
        maxChunksPerEntry = wireIn.read(() -> "maxChunksPerEntry").int32();
        actualChunksPerSegmentTier = wireIn.read(() -> "actualChunksPerSegmentTier").int64();

        segmentHeaderSize = wireIn.read(() -> "segmentHeaderSize").int32();

        tierHashLookupValueBits = wireIn.read(() -> "tierHashLookupValueBits").int32();
        tierHashLookupKeyBits = wireIn.read(() -> "tierHashLookupKeyBits").int32();
        tierHashLookupSlotSize = wireIn.read(() -> "tierHashLookupSlotSize").int32();
        tierHashLookupCapacity = wireIn.read(() -> "tierHashLookupCapacity").int64();
        maxEntriesPerHashLookup = wireIn.read(() -> "maxEntriesPerHashLookup").int64();
        tierHashLookupInnerSize = wireIn.read(() -> "tierHashLookupInnerSize").int64();
        tierHashLookupOuterSize = wireIn.read(() -> "tierHashLookupOuterSize").int64();

        tierFreeListInnerSize = wireIn.read(() -> "tierFreeListInnerSize").int64();
        tierFreeListOuterSize = wireIn.read(() -> "tierFreeListOuterSize").int64();

        tierEntrySpaceInnerSize = wireIn.read(() -> "tierEntrySpaceInnerSize").int64();
        tierEntrySpaceInnerOffset = wireIn.read(() -> "tierEntrySpaceInnerOffset").int32();
        tierEntrySpaceOuterSize = wireIn.read(() -> "tierEntrySpaceOuterSize").int64();

        tierSize = wireIn.read(() -> "tierSize").int64();

        maxExtraTiers = wireIn.read(() -> "maxExtraTiers").int64();
        tierBulkSizeInBytes = wireIn.read(() -> "tierBulkSizeInBytes").int64();
        tierBulkInnerOffsetToTiers = wireIn.read(() -> "tierBulkInnerOffsetToTiers").int64();
        tiersInBulk = wireIn.read(() -> "tiersInBulk").int64();
        log2TiersInBulk = wireIn.read(() -> "log2TiersInBulk").int32();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "dataFileVersion").text(dataFileVersion);

        wireOut.write(() -> "keyClass").typeLiteral(keyClass);
        wireOut.write(() -> "keySizeMarshaller").object(keySizeMarshaller);
        wireOut.write(() -> "keyReader").object(keyReader);
        wireOut.write(() -> "keyDataAccess").object(keyDataAccess);

        wireOut.write(() -> "checksumEntries").bool(checksumEntries);

        wireOut.write(() -> "actualSegments").int32(actualSegments);
        wireOut.write(() -> "hashSplitting").object(hashSplitting);

        wireOut.write(() -> "chunkSize").int64(chunkSize);
        wireOut.write(() -> "maxChunksPerEntry").int32(maxChunksPerEntry);
        wireOut.write(() -> "actualChunksPerSegmentTier").int64(actualChunksPerSegmentTier);

        wireOut.write(() -> "segmentHeaderSize").int32(segmentHeaderSize);

        wireOut.write(() -> "tierHashLookupValueBits").int32(tierHashLookupValueBits);
        wireOut.write(() -> "tierHashLookupKeyBits").int32(tierHashLookupKeyBits);
        wireOut.write(() -> "tierHashLookupSlotSize").int32(tierHashLookupSlotSize);
        wireOut.write(() -> "tierHashLookupCapacity").int64(tierHashLookupCapacity);
        wireOut.write(() -> "maxEntriesPerHashLookup").int64(maxEntriesPerHashLookup);
        wireOut.write(() -> "tierHashLookupInnerSize").int64(tierHashLookupInnerSize);
        wireOut.write(() -> "tierHashLookupOuterSize").int64(tierHashLookupOuterSize);

        wireOut.write(() -> "tierFreeListInnerSize").int64(tierFreeListInnerSize);
        wireOut.write(() -> "tierFreeListOuterSize").int64(tierFreeListOuterSize);

        wireOut.write(() -> "tierEntrySpaceInnerSize").int64(tierEntrySpaceInnerSize);
        wireOut.write(() -> "tierEntrySpaceInnerOffset").int32(tierEntrySpaceInnerOffset);
        wireOut.write(() -> "tierEntrySpaceOuterSize").int64(tierEntrySpaceOuterSize);

        wireOut.write(() -> "tierSize").int64(tierSize);

        wireOut.write(() -> "maxExtraTiers").int64(maxExtraTiers);
        wireOut.write(() -> "tierBulkSizeInBytes").int64(tierBulkSizeInBytes);
        wireOut.write(() -> "tierBulkInnerOffsetToTiers").int64(tierBulkInnerOffsetToTiers);
        wireOut.write(() -> "tiersInBulk").int64(tiersInBulk);
        wireOut.write(() -> "log2TiersInBulk").int32(log2TiersInBulk);
    }

    protected VanillaGlobalMutableState createGlobalMutableState() {
        return Values.newNativeReference(VanillaGlobalMutableState.class);
    }

    public VanillaGlobalMutableState globalMutableState() {
        return globalMutableState;
    }

    private long tierSize() {
        long segmentSize = tierHashLookupOuterSize + TIER_COUNTERS_AREA_SIZE +
                tierFreeListOuterSize + tierEntrySpaceOuterSize;
        if ((segmentSize & 63L) != 0)
            throw new AssertionError();
        return breakL1CacheAssociativityContention(segmentSize);
    }

    protected final long breakL1CacheAssociativityContention(long sizeInBytes) {
        // Conventional alignment to break is 4096 (given Intel's 32KB 8-way L1 cache),
        // for any case break 2 times smaller alignment
        int alignmentToBreak = 2048;
        int eachNthSegmentFallIntoTheSameSet =
                max(1, alignmentToBreak >> numberOfTrailingZeros(sizeInBytes));
        if (eachNthSegmentFallIntoTheSameSet < actualSegments)
            sizeInBytes |= CACHE_LINES.toBytes(1L); // make segment size "odd" (in cache lines)
        return sizeInBytes;
    }

    private long computeNumberOfTiersInBulk() {
        // TODO review heuristics
        int tiersInBulk = actualSegments / 8;
        tiersInBulk = Maths.nextPower2(tiersInBulk, 1);
        while (computeTierBulkBytesSize(tiersInBulk) < OS.pageSize()) {
            tiersInBulk *= 2;
        }
        return tiersInBulk;
    }

    private long computeTierBulkBytesSize(long tiersInBulk) {
        return computeTierBulkInnerOffsetToTiers(tiersInBulk) + tiersInBulk * tierSize;
    }

    protected long computeTierBulkInnerOffsetToTiers(long tiersInBulk) {
        return 0L;
    }

    public void initTransients() {
        initOwnTransients();
    }

    private void initOwnTransients() {
        globalMutableState = createGlobalMutableState();
        tierBulkOffsets = new ArrayList<>();
        switch (tierHashLookupSlotSize) {
            case 4:
                hashLookup = new IntCompactOffHeapLinearHashTable(this);
                break;
            case 8:
                hashLookup = new LongCompactOffHeapLinearHashTable(this);
                break;
            default:
                throw new AssertionError("hash lookup slot size could be 4 or 8, " +
                        tierHashLookupSlotSize + " observed");
        }
        identity = new Identity();
    }

    public final void initBeforeMapping(
            File file, RandomAccessFile raf, long headerEnd, boolean recover) throws IOException {
        this.file = file;
        this.raf = raf;
        this.headerSize = roundUpMapHeaderSize(headerEnd);
        if (!createdOrInMemory) {
            // This block is for reading segmentHeadersOffset before main mapping
            // After the mapping globalMutableState value's bytes are reassigned
            ByteBuffer globalMutableStateBuffer =
                    ByteBuffer.allocate((int) globalMutableState.maxSize());
            FileChannel fileChannel = raf.getChannel();
            while (globalMutableStateBuffer.remaining() > 0) {
                if (fileChannel.read(globalMutableStateBuffer,
                        this.headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET +
                                globalMutableStateBuffer.position()) == -1) {
                    throw throwRecoveryOrReturnIOException(file, "truncated", recover);
                }
            }
            globalMutableStateBuffer.flip();
            //noinspection unchecked
            globalMutableState.bytesStore(BytesStore.wrap(globalMutableStateBuffer), 0,
                    globalMutableState.maxSize());
        }
    }

    public final void createInMemoryStoreAndSegments(ChronicleHashResources resources) {
        this.resources = resources;
        BytesStore bytesStore = nativeBytesStoreWithFixedCapacity(sizeInBytesWithoutTiers());
        createStoreAndSegments(bytesStore);
    }

    private void createStoreAndSegments(BytesStore bytesStore) {
        initBytesStoreAndHeadersViews(bytesStore);
        initOffsetsAndBulks();
    }

    private void initOffsetsAndBulks() {
        segmentHeadersOffset = segmentHeadersOffset();

        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;

        if (createdOrInMemory) {
            zeroOutNewlyMappedChronicleMapBytes();
            // write the segment headers offset after zeroing out
            globalMutableState.setSegmentHeadersOffset(segmentHeadersOffset);
            globalMutableState.setDataStoreSize(sizeInBytesWithoutTiers());
        } else {
            initBulks();
        }
    }

    private void initBulks() {
        if (globalMutableState.getAllocatedExtraTierBulks() > 0) {
            appendBulkData(0, globalMutableState.getAllocatedExtraTierBulks() - 1,
                    bs, sizeInBytesWithoutTiers());
        }
    }

    private void initBytesStoreAndHeadersViews(BytesStore bytesStore) {
        if (bytesStore.start() != 0) {
            throw new AssertionError("bytes store " + bytesStore + " starts from " +
                    bytesStore.start() + ", 0 expected");
        }
        this.bs = bytesStore;
        //noinspection unchecked
        globalMutableState.bytesStore(bs, headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET,
                globalMutableState.maxSize());

        onHeaderCreated();
    }

    public void setResourcesName() {
        resources.setChronicleHashIdentityString(toIdentityString());
    }

    public void registerCleaner() {
        this.cleaner = CleanerUtils.createCleaner(this, resources);
    }

    public void addToOnExitHook() {
        ChronicleHashCloseOnExitHook.add(this);
    }

    public final void createMappedStoreAndSegments(ChronicleHashResources resources)
            throws IOException {
        this.resources = resources;
        createStoreAndSegments(map(dataStoreSize(), 0));
    }

    public final void basicRecover(
            ChronicleHashResources resources, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption)
            throws IOException {
        this.resources = resources;
        long segmentHeadersOffset = globalMutableState().getSegmentHeadersOffset();
        if (segmentHeadersOffset <= 0 || segmentHeadersOffset % 4096 != 0 ||
                segmentHeadersOffset > GIGABYTES.toBytes(1)) {
            segmentHeadersOffset = computeSegmentHeadersOffset();
        }
        long sizeInBytesWithoutTiers = computeSizeInBytesWithoutTiers(segmentHeadersOffset);
        long dataStoreSize = globalMutableState().getDataStoreSize();
        int allocatedExtraTierBulks = globalMutableState().getAllocatedExtraTierBulks();
        if (dataStoreSize < sizeInBytesWithoutTiers ||
                ((dataStoreSize - sizeInBytesWithoutTiers) % tierBulkSizeInBytes != 0)) {
            dataStoreSize = sizeInBytesWithoutTiers + allocatedExtraTierBulks * tierBulkSizeInBytes;
        } else {
            allocatedExtraTierBulks =
                    (int) ((dataStoreSize - sizeInBytesWithoutTiers) / tierBulkSizeInBytes);
        }
        initBytesStoreAndHeadersViews(map(dataStoreSize, 0));

        resetGlobalMutableStateLock(corruptionListener, corruption);
        recoverAllocatedExtraTierBulks(allocatedExtraTierBulks, corruptionListener, corruption);
        recoverSegmentHeadersOffset(segmentHeadersOffset, corruptionListener, corruption);
        recoverDataStoreSize(dataStoreSize, corruptionListener, corruption);
        initOffsetsAndBulks();
    }

    private void resetGlobalMutableStateLock(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        long lockAddr = globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET;
        LockingStrategy lockingStrategy = globalMutableStateLockingStrategy;
        long lockState = lockingStrategy.getState(nativeAccess(), null, lockAddr);
        if (lockState != lockingStrategy.resetState()) {
            report(corruptionListener, corruption, -1, () ->
                    format("global mutable state lock of map at {} is not clear: {}",
                            file, lockingStrategy.toString(lockState))
            );
            lockingStrategy.reset(nativeAccess(), null, lockAddr);
        }
    }

    private void recoverAllocatedExtraTierBulks(
            int allocatedExtraTierBulks, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        if (globalMutableState.getAllocatedExtraTierBulks() != allocatedExtraTierBulks) {
            report(corruptionListener, corruption, -1, () ->
                    format("allocated extra tier bulks counter corrupted, or the map file {} " +
                                    "is truncated. stored: {}, should be: {}",
                            file, globalMutableState.getAllocatedExtraTierBulks(),
                            allocatedExtraTierBulks)
            );
            globalMutableState.setAllocatedExtraTierBulks(allocatedExtraTierBulks);
        }
    }

    private void recoverSegmentHeadersOffset(
            long segmentHeadersOffset, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        if (globalMutableState.getSegmentHeadersOffset() != segmentHeadersOffset) {
            report(corruptionListener, corruption, -1, () ->
                    format("segment headers offset of map at {} corrupted. stored: {}, should be: {}",
                            file, globalMutableState.getSegmentHeadersOffset(), segmentHeadersOffset)
            );
            globalMutableState.setSegmentHeadersOffset(segmentHeadersOffset);
        }
    }

    private void recoverDataStoreSize(
            long dataStoreSize, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        if (globalMutableState.getDataStoreSize() != dataStoreSize) {
            report(corruptionListener, corruption, -1, () ->
                    format("data store size of map at {} corrupted. stored: {}, should be: {}",
                            file, globalMutableState.getDataStoreSize(), dataStoreSize)
            );
            globalMutableState.setDataStoreSize(dataStoreSize);
        }
    }

    private boolean persisted() {
        return file != null;
    }

    /**
     * newly-extended file contents are not guaranteed to be zero
     */
    protected void zeroOutNewlyMappedChronicleMapBytes() {
        zeroOutGlobalMutableState();
        zeroOutSegmentHeaders();
        zeroOutFirstSegmentTiers();
    }

    private void zeroOutGlobalMutableState() {
        bs.zeroOut(headerSize, headerSize + globalMutableStateTotalUsedSize());
    }

    protected long globalMutableStateTotalUsedSize() {
        return GLOBAL_MUTABLE_STATE_VALUE_OFFSET + globalMutableState().maxSize();
    }

    private void zeroOutSegmentHeaders() {
        bs.zeroOut(segmentHeadersOffset, segmentsOffset);
    }

    private void zeroOutFirstSegmentTiers() {
        for (int segmentIndex = 0; segmentIndex < segments(); segmentIndex++) {
            long segmentOffset = segmentOffset(segmentIndex);
            zeroOutNewlyMappedTier(bs, segmentOffset);
        }
    }

    private void zeroOutNewlyMappedTier(BytesStore bytesStore, long tierOffset) {
        // Zero out hash lookup, tier data and free list bit set. Leave entry space dirty.
        bytesStore.zeroOut(tierOffset, tierOffset + tierSize - tierEntrySpaceOuterSize);
    }

    public void onHeaderCreated() {
    }

    /**
     * @return the version of Chronicle Map that was used to create the current data file
     */
    public String persistedDataVersion() {
        return dataFileVersion;
    }

    private long segmentHeadersOffset() {
        if (createdOrInMemory) {
            return computeSegmentHeadersOffset();
        } else {
            return globalMutableState.getSegmentHeadersOffset();
        }
    }

    private long computeSegmentHeadersOffset() {
        long reserved = RESERVED_GLOBAL_MUTABLE_STATE_BYTES - globalMutableStateTotalUsedSize();
        // Align segment headers on page boundary to minimize number of pages that
        // segment headers span
        return pageAlign(mapHeaderInnerSize() + reserved);
    }

    public long mapHeaderInnerSize() {
        return headerSize + globalMutableStateTotalUsedSize();
    }

    @Override
    public File file() {
        return file;
    }

    public final long sizeInBytesWithoutTiers() {
        return computeSizeInBytesWithoutTiers(segmentHeadersOffset());
    }

    private long computeSizeInBytesWithoutTiers(long segmentHeadersOffset) {
        return segmentHeadersOffset + actualSegments * (segmentHeaderSize + tierSize);
    }

    public final long dataStoreSize() {
        long sizeInBytesWithoutTiers = sizeInBytesWithoutTiers();
        int allocatedExtraTierBulks = !createdOrInMemory ?
                globalMutableState.getAllocatedExtraTierBulks() : 0;
        return sizeInBytesWithoutTiers + allocatedExtraTierBulks * tierBulkSizeInBytes;
    }

    @Override
    public final void close() {
        if (resources.releaseManually()) {
            cleanupOnClose();
        }
    }

    protected void cleanupOnClose() {
        // Releases nothing after resources.releaseManually(), only removes the cleaner
        // from the internal linked list of all cleaners.
        cleaner.clean();
        ChronicleHashCloseOnExitHook.remove(this);
        // Make GC life easier
        keyReader = null;
        keyDataAccess = null;
    }

    @Override
    public boolean isOpen() {
        return !resources.closed();
    }

    public final void checkKey(Object key) {
        if (!keyClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException(toIdentityString() + ": Key must be a " +
                    keyClass.getName() + " but was a " + key.getClass());
        }
    }

    public final long segmentHeaderAddress(int segmentIndex) {
        return bsAddress() + segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    public long bsAddress() {
        return bs.addressForRead(0);
    }

    public final long segmentBaseAddr(int segmentIndex) {
        return bsAddress() + segmentOffset(segmentIndex);
    }

    private long segmentOffset(long segmentIndex) {
        return segmentsOffset + segmentIndex * tierSize;
    }

    public final int inChunks(long sizeInBytes) {
        // TODO optimize for the case when chunkSize is power of 2, that is default (and often) now
        if (sizeInBytes <= chunkSize)
            return 1;

        // todo: we have added padding to prevent the chunks getting corrupted see - net.openhft.chronicle.map.MissSizedMapsTest

        // int division is MUCH faster than long on Intel CPUs
        if (sizeInBytes <= Integer.MAX_VALUE)
            return (((int) sizeInBytes) / (int) chunkSize) + 1;
        return (int) (sizeInBytes / chunkSize) + 1;
    }

    public final int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    @Override
    public int segments() {
        return actualSegments;
    }

    private long globalMutableStateAddress() {
        return bsAddress() + headerSize;
    }

    public void globalMutableStateLock() {
        globalMutableStateLockAcquisitionStrategy.acquire(
                globalMutableStateLockTryAcquireOperation, globalMutableStateLockingStrategy,
                nativeAccess(), null,
                globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET);
    }

    public void globalMutableStateUnlock() {
        globalMutableStateLockingStrategy.unlock(nativeAccess(), null,
                globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET);
    }

    /**
     * For tests
     */
    public boolean hasExtraTierBulks() {
        return globalMutableState.getAllocatedExtraTierBulks() > 0;
    }

    @Override
    public long offHeapMemoryUsed() {
        return resources.totalMemory();
    }

    public long allocateTier() {
        globalMutableStateLock();
        try {
            long tiersInUse = globalMutableState.getExtraTiersInUse();
            if (tiersInUse >= maxExtraTiers) {
                throw new IllegalStateException(toIdentityString() + ": " +
                        "Attempt to allocate #" + (tiersInUse + 1) +
                        " extra segment tier, " + maxExtraTiers + " is maximum.\n" +
                        "Possible reasons include:\n" +
                        " - you have forgotten to configure (or configured wrong) " +
                        "builder.entries() number\n" +
                        " - same regarding other sizing Chronicle Hash configurations, most " +
                        "likely maxBloatFactor(), averageKeySize(), or averageValueSize()\n" +
                        " - keys, inserted into the ChronicleHash, are distributed suspiciously " +
                        "bad. This might be a DOS attack");
            }
            long firstFreeTierIndex = globalMutableState.getFirstFreeTierIndex();
            if (firstFreeTierIndex < 0) {
                throw new RuntimeException(toIdentityString() +
                        ": unexpected firstFreeTierIndex value " + firstFreeTierIndex);
            }
            if (firstFreeTierIndex == 0) {
                try {
                    allocateTierBulk();
                } catch (IOException e) {
                    throw new RuntimeException(toIdentityString(), e);
                }
                firstFreeTierIndex = globalMutableState.getFirstFreeTierIndex();
                if (firstFreeTierIndex <= 0) {
                    throw new RuntimeException(toIdentityString() +
                            ": unexpected firstFreeTierIndex value " + firstFreeTierIndex);
                }
            }
            globalMutableState.setExtraTiersInUse(tiersInUse + 1);
            BytesStore allocatedTierBytes = tierBytesStore(firstFreeTierIndex);
            long allocatedTierOffset = tierBytesOffset(firstFreeTierIndex);
            long tierBaseAddr = allocatedTierBytes.addressForRead(0) + allocatedTierOffset;
            long tierCountersAreaAddr = tierBaseAddr + tierHashLookupOuterSize;
            long nextFreeTierIndex = TierCountersArea.nextTierIndex(tierCountersAreaAddr);
            globalMutableState.setFirstFreeTierIndex(nextFreeTierIndex);
            return firstFreeTierIndex;
        } finally {
            globalMutableStateUnlock();
        }
    }

    private void allocateTierBulk() throws IOException {
        int allocatedExtraTierBulks = globalMutableState.getAllocatedExtraTierBulks();

        mapTierBulks(allocatedExtraTierBulks);

        long firstTierIndex = extraTierIndexToTierIndex(allocatedExtraTierBulks * tiersInBulk);
        BytesStore tierBytesStore = tierBytesStore(firstTierIndex);
        long firstTierOffset = tierBytesOffset(firstTierIndex);
        if (tierBulkInnerOffsetToTiers > 0) {
            // These bytes are bit sets in Replicated version
            tierBytesStore.zeroOut(firstTierOffset - tierBulkInnerOffsetToTiers, firstTierOffset);
        }

        long lastTierIndex = firstTierIndex + tiersInBulk - 1;
        linkAndZeroOutFreeTiers(firstTierIndex, lastTierIndex);

        // see HCOLL-397
        if (persisted()) {
            long address = tierBytesStore.addressForRead(firstTierOffset - tierBulkInnerOffsetToTiers);
            long endAddress = tierBytesStore.addressForRead(tierBytesOffset(lastTierIndex)) + tierSize;
            long length = endAddress - address;
            msync(address, length);
        }

        // after we are sure the new bulk is initialized, update the global mutable state
        globalMutableState.setAllocatedExtraTierBulks(allocatedExtraTierBulks + 1);
        globalMutableState.setFirstFreeTierIndex(firstTierIndex);
        globalMutableState.addDataStoreSize(tierBulkSizeInBytes);
    }

    public void msync() throws IOException {
        if (persisted()) {
            msync(bsAddress(), bs.capacity());
        }
    }

    private void msync(long address, long length) throws IOException {
        // address should be a multiple of page size
        if (OS.pageAlign(address) != address) {
            long oldAddress = address;
            address = OS.pageAlign(address) - OS.pageSize();
            length += oldAddress - address;
        }
        if (OS.isWindows()) {
            WindowsMsync.msync(raf, address, length);
        } else {
            PosixMsync.msync(address, length);
        }
    }

    public void linkAndZeroOutFreeTiers(long firstTierIndex, long lastTierIndex) {
        for (long tierIndex = firstTierIndex; tierIndex <= lastTierIndex; tierIndex++) {
            long tierOffset = tierBytesOffset(tierIndex);
            BytesStore tierBytesStore = tierBytesStore(tierIndex);
            zeroOutNewlyMappedTier(tierBytesStore, tierOffset);
            if (tierIndex < lastTierIndex) {
                long tierCountersAreaOffset = tierOffset + tierHashLookupOuterSize;
                TierCountersArea.nextTierIndex(tierBytesStore.addressForRead(0) + tierCountersAreaOffset,
                        tierIndex + 1);
            }
        }
    }

    public long extraTierIndexToTierIndex(long extraTierIndex) {
        return actualSegments + extraTierIndex + 1;
    }

    public long tierIndexToBaseAddr(long tierIndex) {
        // tiers are 1-counted, to allow tierIndex = 0 to be un-initialized in off-heap memory,
        // convert into 0-based form
        long tierIndexMinusOne = tierIndex - 1;
        if (tierIndexMinusOne < actualSegments)
            return segmentBaseAddr((int) tierIndexMinusOne);
        return extraTierIndexToBaseAddr(tierIndexMinusOne);
    }

    public BytesStore tierBytesStore(long tierIndex) {
        long tierIndexMinusOne = tierIndex - 1;
        if (tierIndexMinusOne < actualSegments)
            return bs;
        return tierBulkData(tierIndexMinusOne).bytesStore;
    }

    public long tierBytesOffset(long tierIndex) {
        long tierIndexMinusOne = tierIndex - 1;
        if (tierIndexMinusOne < actualSegments)
            return segmentOffset(tierIndexMinusOne);
        long extraTierIndex = tierIndexMinusOne - actualSegments;
        int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
        if (bulkIndex >= tierBulkOffsets.size())
            mapTierBulks(bulkIndex);
        return tierBulkOffsets.get(bulkIndex).offset + tierBulkInnerOffsetToTiers +
                (extraTierIndex & (tiersInBulk - 1)) * tierSize;
    }

    private TierBulkData tierBulkData(long tierIndexMinusOne) {
        long extraTierIndex = tierIndexMinusOne - actualSegments;
        int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
        if (bulkIndex >= tierBulkOffsets.size())
            mapTierBulks(bulkIndex);
        return tierBulkOffsets.get(bulkIndex);
    }

    private long extraTierIndexToBaseAddr(long tierIndexMinusOne) {
        long extraTierIndex = tierIndexMinusOne - actualSegments;
        int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
        if (bulkIndex >= tierBulkOffsets.size())
            mapTierBulks(bulkIndex);
        TierBulkData tierBulkData = tierBulkOffsets.get(bulkIndex);
        long tierIndexOffsetWithinBulk = extraTierIndex & (tiersInBulk - 1);
        return tierAddr(tierBulkData, tierIndexOffsetWithinBulk);
    }

    protected long tierAddr(TierBulkData tierBulkData, long tierIndexOffsetWithinBulk) {
        return tierBulkData.bytesStore.addressForRead(0) + tierBulkData.offset +
                tierBulkInnerOffsetToTiers + tierIndexOffsetWithinBulk * tierSize;
    }

    private void mapTierBulks(int upToBulkIndex) {
        if (persisted()) {
            try {
                mapTierBulksMapped(upToBulkIndex);
            } catch (IOException e) {
                throw new RuntimeException(toIdentityString(), e);
            }
        } else {
            // in-memory ChMap
            allocateTierBulks(upToBulkIndex);
        }
    }

    private void mapTierBulksMapped(int upToBulkIndex) throws IOException {
        int firstBulkToMapIndex = tierBulkOffsets.size();
        int bulksToMap = upToBulkIndex + 1 - firstBulkToMapIndex;
        long mapSize = bulksToMap * tierBulkSizeInBytes;
        long mappingOffsetInFile, firstBulkToMapOffsetWithinMapping;
        long firstBulkToMapOffset = bulkOffset(firstBulkToMapIndex);
        if (OS.mapAlign(firstBulkToMapOffset) == firstBulkToMapOffset) {
            mappingOffsetInFile = firstBulkToMapOffset;
            firstBulkToMapOffsetWithinMapping = 0;
        } else {
            // If the bulk was allocated on OS with 4K mapping granularity (linux) and we
            // are mapping it in OS with 64K mapping granularity (windows), we might need to
            // start the mapping earlier than the first tier to map actually starts
            mappingOffsetInFile = OS.mapAlign(firstBulkToMapOffset) - OS.mapAlignment();
            firstBulkToMapOffsetWithinMapping = firstBulkToMapOffset - mappingOffsetInFile;
            // Now might need to have bigger mapSize
            mapSize += firstBulkToMapOffsetWithinMapping;
        }
        // mapping by hand, because MappedFile/MappedBytesStore doesn't allow to create a BS
        // which starts not from the beginning of the file, but has start() of 0
        NativeBytesStore extraStore = map(mapSize, mappingOffsetInFile);
        appendBulkData(firstBulkToMapIndex, upToBulkIndex, extraStore,
                firstBulkToMapOffsetWithinMapping);
    }

    /**
     * @see net.openhft.chronicle.bytes.MappedFile#acquireByteStore(long, MappedBytesStoreFactory)
     */
    private NativeBytesStore map(long mapSize, long mappingOffsetInFile) throws IOException {
        mapSize = pageAlign(mapSize);
        long minFileSize = mappingOffsetInFile + mapSize;
        FileChannel fileChannel = raf.getChannel();
        if (fileChannel.size() < minFileSize) {
            // In MappedFile#acquireByteStore(), this is wrapped with fileLock(), to avoid race
            // condition between processes. This map() method is called either when a new tier is
            // allocated (in this case concurrent access is mutually excluded by
            // globalMutableStateLock), or on map creation, when race condition should be excluded
            // by self-bootstrapping header spec
            raf.setLength(minFileSize);

            // RandomAccessFile#setLength() only calls ftruncate,
            // which will not preallocate space on XFS filesystem of Linux.
            // And writing that file will create a sparse file with a large number of extents.
            // This kind of fragmented file may hang the program and cause dmesg reports
            // "XFS: ... possible memory allocation deadlock size ... in kmem_alloc (mode:0x250)".
            // We can fix this by trying calling posix_fallocate to preallocate the space.
            if (OS.isLinux()) {
                PosixFallocate.fallocate(raf.getFD(), 0, minFileSize);
            }
        }
        long address = OS.map(fileChannel, READ_WRITE, mappingOffsetInFile, mapSize);
        resources.addMemoryResource(address, mapSize);
        return new NativeBytesStore(address, mapSize, null, false);
    }

    private long bulkOffset(int bulkIndex) {
        return sizeInBytesWithoutTiers() + bulkIndex * tierBulkSizeInBytes;
    }

    private void allocateTierBulks(int upToBulkIndex) {
        int firstBulkToAllocateIndex = tierBulkOffsets.size();
        int bulksToAllocate = upToBulkIndex + 1 - firstBulkToAllocateIndex;
        long allocationSize = bulksToAllocate * tierBulkSizeInBytes;
        BytesStore extraStore = nativeBytesStoreWithFixedCapacity(allocationSize);
        appendBulkData(firstBulkToAllocateIndex, upToBulkIndex, extraStore, 0);
    }

    private BytesStore nativeBytesStoreWithFixedCapacity(long capacity) {
        long address = OS.memory().allocate(capacity);
        resources.addMemoryResource(address, capacity);
        return new NativeBytesStore<>(address, capacity, null, false);
    }

    private void appendBulkData(int firstBulkToMapIndex, int upToBulkIndex, BytesStore extraStore,
                                long offsetWithinMapping) {
        TierBulkData firstMappedBulkData = new TierBulkData(extraStore, offsetWithinMapping);
        tierBulkOffsets.add(firstMappedBulkData);
        for (int bulkIndex = firstBulkToMapIndex + 1; bulkIndex <= upToBulkIndex; bulkIndex++) {
            tierBulkOffsets.add(new TierBulkData(firstMappedBulkData,
                    offsetWithinMapping += tierBulkSizeInBytes));
        }
    }

    protected void addContext(ContextHolder contextHolder) {
        resources.addContext(contextHolder);
    }

    public void addCloseable(Closeable closeable) {
        resources.addCloseable(closeable);
    }

    /**
     * For testing only
     */
    public List<WeakReference<ContextHolder>> allContexts() {
        return Collections.unmodifiableList(resources.contexts());
    }

    public static class TierBulkData {
        public final BytesStore bytesStore;
        public final long offset;

        public TierBulkData(BytesStore bytesStore, long offset) {
            this.bytesStore = bytesStore;
            this.offset = offset;
        }

        public TierBulkData(TierBulkData data, long offset) {
            this.bytesStore = data.bytesStore;
            this.offset = offset;
        }
    }

    /**
     * {@link ChronicleHashCloseOnExitHook} needs to use {@code VanillaChronicleHash}es as
     * WeakHashMap keys, but with identity comparison, not Map's equals() and hashCode().
     */
    public class Identity {
        public VanillaChronicleHash hash() {
            return VanillaChronicleHash.this;
        }
    }
}
