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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.algo.locks.*;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static net.openhft.chronicle.algo.MemoryUnit.*;
import static net.openhft.chronicle.bytes.NativeBytesStore.lazyNativeBytesStoreWithFixedCapacity;
import static net.openhft.chronicle.core.OS.pageAlign;
import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.*;
import static net.openhft.chronicle.hash.impl.DummyReferenceCounted.DUMMY_REFERENCE_COUNTED;

public abstract class VanillaChronicleHash<K,
        C extends HashEntry<K>, SC extends HashSegmentContext<K, ?>,
        ECQ extends ExternalHashQueryContext<K>>
        implements ChronicleHash<K, C, SC, ECQ>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleHash.class);

    private static final long serialVersionUID = 0L;

    public static final long TIER_COUNTERS_AREA_SIZE = 64;
    public static final long RESERVED_GLOBAL_MUTABLE_STATE_BYTES = 1024;

    /////////////////////////////////////////////////
    // Version
    public final String dataFileVersion;

    /////////////////////////////////////////////////
    // If the hash was created in the first place, or read from disk
    public transient boolean createdOrInMemory = false;

    /////////////////////////////////////////////////
    // Key Data model
    public final Class<K> kClass;
    public final SizeMarshaller keySizeMarshaller;
    public final SizedReader<K> originalKeyReader;
    public final DataAccess<K> originalKeyDataAccess;

    /////////////////////////////////////////////////
    // Checksum entries
    public final boolean checksumEntries;

    /////////////////////////////////////////////////
    // Concurrency (number of segments), memory management and dependent fields
    public final int actualSegments;
    public final HashSplitting hashSplitting;

    public final long entriesPerSegment;

    public final long chunkSize;
    public final int maxChunksPerEntry;
    public final long actualChunksPerSegment;

    /////////////////////////////////////////////////
    // Precomputed offsets and sizes for fast Context init
    final int segmentHeaderSize;

    public final int segmentHashLookupValueBits;
    public final int segmentHashLookupKeyBits;
    public final int segmentHashLookupEntrySize;
    public final long segmentHashLookupCapacity;
    final long segmentHashLookupInnerSize;
    public final long segmentHashLookupOuterSize;

    public final long segmentFreeListInnerSize;
    public final long segmentFreeListOuterSize;

    final long segmentEntrySpaceInnerSize;
    public final int segmentEntrySpaceInnerOffset;
    final long segmentEntrySpaceOuterSize;

    public final long segmentSize;

    final long maxExtraTiers;
    final long tierBulkSizeInBytes;
    final long tierBulkInnerOffsetToTiers;
    protected final long tiersInBulk;
    protected final int log2TiersInBulk;

    /////////////////////////////////////////////////
    // Bytes Store (essentially, the base address) and serialization-dependent offsets
    private transient File file;
    protected transient BytesStore bs;

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

    public transient List<TierBulkData> tierBulkOffsets;

    public transient long headerSize;
    transient long segmentHeadersOffset;
    transient long segmentsOffset;

    public transient CompactOffHeapLinearHashTable hashLookup;

    protected transient volatile boolean closed = false;

    private transient VanillaGlobalMutableState globalMutableState;

    public VanillaChronicleHash(ChronicleMapBuilder<K, ?> builder) {
        // Version
        dataFileVersion = BuildVersion.version();

        // Because we are in constructor. If the Hash loaded from persistence file, deserialization
        // bypasses the constructor and createdOrInMemory = false
        this.createdOrInMemory = true;

        @SuppressWarnings("deprecation")
        ChronicleHashBuilderPrivateAPI<K> privateAPI = builder.privateAPI();

        // Data model
        SerializationBuilder<K> keyBuilder = privateAPI.keyBuilder();
        kClass = keyBuilder.tClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();
        originalKeyDataAccess = keyBuilder.dataAccess();

        actualSegments = privateAPI.actualSegments();
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);

        entriesPerSegment = privateAPI.entriesPerSegment();

        chunkSize = privateAPI.chunkSize();
        maxChunksPerEntry = privateAPI.maxChunksPerEntry();
        actualChunksPerSegment = privateAPI.actualChunksPerSegment();

        // Precomputed offsets and sizes for fast Context init
        segmentHeaderSize = privateAPI.segmentHeaderSize();

        segmentHashLookupValueBits = valueBits(actualChunksPerSegment);
        segmentHashLookupKeyBits = keyBits(entriesPerSegment, segmentHashLookupValueBits);
        segmentHashLookupEntrySize =
                entrySize(segmentHashLookupKeyBits, segmentHashLookupValueBits);
        if (!privateAPI.aligned64BitMemoryOperationsAtomic() && segmentHashLookupEntrySize > 4) {
            throw new IllegalStateException("aligned64BitMemoryOperationsAtomic() == false, " +
                    "but hash lookup slot is " + segmentHashLookupEntrySize);
        }
        segmentHashLookupCapacity = CompactOffHeapLinearHashTable.capacityFor(entriesPerSegment);
        segmentHashLookupInnerSize = segmentHashLookupCapacity * segmentHashLookupEntrySize;
        segmentHashLookupOuterSize = CACHE_LINES.align(segmentHashLookupInnerSize, BYTES);

        segmentFreeListInnerSize = LONGS.align(
                BYTES.alignAndConvert(actualChunksPerSegment, BITS), BYTES);
        segmentFreeListOuterSize = CACHE_LINES.align(segmentFreeListInnerSize, BYTES);

        segmentEntrySpaceInnerSize = chunkSize * actualChunksPerSegment;
        segmentEntrySpaceInnerOffset = privateAPI.segmentEntrySpaceInnerOffset();
        segmentEntrySpaceOuterSize = CACHE_LINES.align(
                segmentEntrySpaceInnerOffset + segmentEntrySpaceInnerSize, BYTES);

        segmentSize = segmentSize();

        maxExtraTiers = privateAPI.maxExtraTiers();
        tiersInBulk = computeNumberOfTiersInBulk();
        log2TiersInBulk = Maths.intLog2(tiersInBulk);
        tierBulkInnerOffsetToTiers = computeTierBulkInnerOffsetToTiers(tiersInBulk);
        tierBulkSizeInBytes = computeTierBulkBytesSize(tiersInBulk);

        checksumEntries = privateAPI.checksumEntries();
    }

    protected VanillaGlobalMutableState createGlobalMutableState() {
        return Values.newNativeReference(VanillaGlobalMutableState.class);
    }

    protected VanillaGlobalMutableState globalMutableState() {
        return globalMutableState;
    }

    private long segmentSize() {
        long ss = segmentHashLookupOuterSize + TIER_COUNTERS_AREA_SIZE +
                segmentFreeListOuterSize + segmentEntrySpaceOuterSize;
        if ((ss & 63L) != 0)
            throw new AssertionError();
        return breakL1CacheAssociativityContention(ss);
    }

    private long breakL1CacheAssociativityContention(long segmentSize) {
        // Conventional alignment to break is 4096 (given Intel's 32KB 8-way L1 cache),
        // for any case break 2 times smaller alignment
        int alignmentToBreak = 2048;
        int eachNthSegmentFallIntoTheSameSet =
                max(1, alignmentToBreak >> numberOfTrailingZeros(segmentSize));
        if (eachNthSegmentFallIntoTheSameSet < actualSegments) {
            segmentSize |= CACHE_LINES.toBytes(1L); // make segment size "odd" (in cache lines)
        }
        return segmentSize;
    }

    private long computeNumberOfTiersInBulk() {
        // TODO review heuristics
        int tiersInBulk = actualSegments / 8;
        tiersInBulk = Maths.nextPower2(tiersInBulk, 1);
        while (segmentSize * tiersInBulk < OS.pageSize()) {
            tiersInBulk *= 2;
        }
        return tiersInBulk;
    }

    private long computeTierBulkBytesSize(long tiersInBulk) {
        return computeTierBulkInnerOffsetToTiers(tiersInBulk) + tiersInBulk * segmentSize;
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
        if (segmentHashLookupEntrySize == 4) {
            hashLookup = new IntCompactOffHeapLinearHashTable(this);
        } else if (segmentHashLookupEntrySize == 8) {
            hashLookup = new LongCompactOffHeapLinearHashTable(this);
        } else {
            throw new AssertionError("hash lookup slot size could be 4 or 8, " +
                    segmentHashLookupEntrySize + " observed");
        }
    }

    public final void initBeforeMapping(FileChannel ch) throws IOException {
        headerSize = roundUpMapHeaderSize(ch.position());
        if (!createdOrInMemory) {
            // This block is for reading segmentHeadersOffset before main mapping
            // After the mapping globalMutableState value's bytes are reassigned
            ch.position(headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET);
            ByteBuffer globalMutableStateBuffer =
                    ByteBuffer.allocate((int) globalMutableState.maxSize());
            ch.read(globalMutableStateBuffer);
            globalMutableStateBuffer.flip();
            globalMutableState.bytesStore(BytesStore.wrap(globalMutableStateBuffer), 0,
                    globalMutableState.maxSize());
        }
    }

    private static long roundUpMapHeaderSize(long headerSize) {
        long roundUp = (headerSize + 127L) & ~127L;
        if (roundUp - headerSize < 64)
            roundUp += 128;
        return roundUp;
    }

    public final void createMappedStoreAndSegments(BytesStore bytesStore) throws IOException {
        if (bytesStore.start() != 0) {
            throw new AssertionError("bytes store " + bytesStore + " starts from " +
                    bytesStore.start() + ", 0 expected");
        }
        this.bs = bytesStore;
        globalMutableState.bytesStore(bs, headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET,
                globalMutableState.maxSize());

        onHeaderCreated();

        segmentHeadersOffset = segmentHeadersOffset();

        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;

        if (createdOrInMemory) {
            zeroOutNewlyMappedChronicleMapBytes();
            // write the segment headers offset after zeroing out
            globalMutableState.setSegmentHeadersOffset(segmentHeadersOffset);
        } else {
            if (globalMutableState.getAllocatedExtraTierBulks() > 0) {
                appendBulkData(0, globalMutableState.getAllocatedExtraTierBulks() - 1,
                        bs, sizeInBytesWithoutTiers());
            }
        }
    }

    public final void createMappedStoreAndSegments(File file) throws IOException {
        // TODO this method had been moved -- not clear where
        //OS.warnOnWindows(sizeInBytesWithoutTiers());
        this.file = file;
        long mapSize = createdOrInMemory ? pageAlign(sizeInBytesWithoutTiers()) :
                expectedFileSize();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            createMappedStoreAndSegments(map(raf, mapSize, 0));
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
            long segmentOffset = msBytesSegmentOffset(segmentIndex);
            zeroOutNewlyMappedTier(bs, segmentOffset);
        }
    }

    private void zeroOutNewlyMappedTier(BytesStore bytesStore, long segmentOffset) {
        // Zero out hash lookup, tier data and free list bit set. Leave entry space.
        bytesStore.zeroOut(segmentOffset, segmentOffset + segmentSize - segmentEntrySpaceOuterSize);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initOwnTransients();
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
            // Align segment headers on page boundary to minimize number of pages that
            // segment headers span
            long reserved = RESERVED_GLOBAL_MUTABLE_STATE_BYTES - globalMutableStateTotalUsedSize();
            return pageAlign(mapHeaderInnerSize() + reserved);
        } else {
            return globalMutableState.getSegmentHeadersOffset();
        }
    }

    public long mapHeaderInnerSize() {
        return headerSize + globalMutableStateTotalUsedSize();
    }

    @Override
    public File file() {
        return file;
    }

    public final long sizeInBytesWithoutTiers() {
        return segmentHeadersOffset() + actualSegments * (segmentHeaderSize + segmentSize);
    }

    public final long expectedFileSize() {
        long sizeInBytesWithoutTiers = sizeInBytesWithoutTiers();
        int allocatedExtraTierBulks = globalMutableState.getAllocatedExtraTierBulks();
        return pageAlign(sizeInBytesWithoutTiers + allocatedExtraTierBulks * tierBulkSizeInBytes);
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
        closed = true;
        bs = null;
        file = null;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    public final void checkKey(Object key) {
        if (!kClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException("Key must be a " + kClass.getName() +
                    " but was a " + key.getClass());
        }
    }

    /**
     * For testing
     */
    public final long[] segmentSizes() {
        long[] sizes = new long[actualSegments];
        for (int i = 0; i < actualSegments; i++) {
            sizes[i] = BigSegmentHeader.INSTANCE.size(segmentHeaderAddress(i));
        }
        return sizes;
    }

    public final long segmentHeaderAddress(int segmentIndex) {
        return bsAddress() + segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    public long bsAddress() {
        return bs.address(0);
    }

    public final long segmentBaseAddr(int segmentIndex) {
        return bsAddress() + msBytesSegmentOffset(segmentIndex);
    }

    private long msBytesSegmentOffset(long segmentIndex) {
        return segmentsOffset + segmentIndex * segmentSize;
    }

    public final int inChunks(long sizeInBytes) {
        // TODO optimize for the case when chunkSize is power of 2, that is default (and often) now
        if (sizeInBytes <= chunkSize)
            return 1;
        // int division is MUCH faster than long on Intel CPUs
        sizeInBytes -= 1L;
        if (sizeInBytes <= Integer.MAX_VALUE)
            return (((int) sizeInBytes) / (int) chunkSize) + 1;
        return (int) (sizeInBytes / chunkSize) + 1;
    }

    @Override
    public final long longSize() {
        long result = 0L;
        for (int i = 0; i < actualSegments; i++) {
            long segmentHeaderAddress = segmentHeaderAddress(i);
            result += BigSegmentHeader.INSTANCE.size(segmentHeaderAddress) -
                    BigSegmentHeader.INSTANCE.deleted(segmentHeaderAddress);
        }
        return result;
    }

    public final int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    @Override
    public int segments() {
        return actualSegments;
    }

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

    private long globalMutableStateAddress() {
        return bsAddress() + headerSize;
    }

    public void globalMutableStateLock() {
        globalMutableStateLockAcquisitionStrategy.acquire(
                globalMutableStateLockTryAcquireOperation, globalMutableStateLockingStrategy,
                Access.nativeAccess(), null,
                globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET);
    }

    public void globalMutableStateUnlock() {
        globalMutableStateLockingStrategy.unlock(Access.nativeAccess(), null,
                globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET);
    }

    public long allocateTier(int forSegmentIndex, int tier) {
        LOG.debug("Allocate tier for segment # {}, tier {}", forSegmentIndex, tier);
        globalMutableStateLock();
        try {
            long tiersInUse = globalMutableState.getExtraTiersInUse();
            if (tiersInUse == maxExtraTiers) {
                throw new IllegalStateException("Attempt to allocate " + (maxExtraTiers + 1) +
                        "th extra segment tier, " + maxExtraTiers + " is maximum.\n" +
                        "Possible reasons include:\n" +
                        " - you have forgotten to configure (or configured wrong) " +
                        "builder.entries() number\n" +
                        " - same regarding other sizing Chronicle Hash configurations, most " +
                        "likely maxBloatFactor(), averageKeySize(), or averageValueSize()\n" +
                        " - keys, inserted into the ChronicleHash, are distributed suspiciously " +
                        "bad. This might be a DOS attack");
            }
            while (true) {
                long firstFreeTierIndex = globalMutableState.getFirstFreeTierIndex();
                if (firstFreeTierIndex > 0) {
                    globalMutableState.setExtraTiersInUse(tiersInUse + 1);
                    BytesStore allocatedTierBytes = tierBytesStore(firstFreeTierIndex);
                    long allocatedTierOffset = tierBytesOffset(firstFreeTierIndex);
                    long tierBaseAddr = allocatedTierBytes.address(0) + allocatedTierOffset;
                    long tierCountersAreaAddr = tierBaseAddr + segmentHashLookupOuterSize;
                    long nextFreeTierIndex = TierCountersArea.nextTierIndex(tierCountersAreaAddr);
                    // now, when this tier will be a part of the map, the next tier designates
                    // the next tier in the data structure, should be 0
                    TierCountersArea.nextTierIndex(tierCountersAreaAddr, 0);
                    TierCountersArea.segmentIndex(tierCountersAreaAddr, forSegmentIndex);
                    TierCountersArea.tier(tierCountersAreaAddr, tier);
                    globalMutableState.setFirstFreeTierIndex(nextFreeTierIndex);
                    return firstFreeTierIndex;
                } else {
                    allocateTierBulk();
                }
            }
        } finally {
            globalMutableStateUnlock();
        }
    }

    private void allocateTierBulk() {
        int allocatedExtraTierBulks = globalMutableState.getAllocatedExtraTierBulks();
        mapTierBulks(allocatedExtraTierBulks);

        // integer overflow aware
        long firstTierIndex = actualSegments + 1L + ((long) allocatedExtraTierBulks) * tiersInBulk;
        BytesStore tierBytesStore = tierBytesStore(firstTierIndex);
        long firstTierOffset = tierBytesOffset(firstTierIndex);
        if (tierBulkInnerOffsetToTiers > 0) {
            // These bytes are bit sets in Replicated version
            tierBytesStore.zeroOut(firstTierOffset - tierBulkInnerOffsetToTiers, firstTierOffset);
        }

        // Link newly allocated tiers into free tiers list
        long lastTierIndex = firstTierIndex + tiersInBulk - 1;
        for (long tierIndex = firstTierIndex; tierIndex <= lastTierIndex; tierIndex++) {
            long tierOffset = tierBytesOffset(tierIndex);
            zeroOutNewlyMappedTier(tierBytesStore, tierOffset);
            if (tierIndex < lastTierIndex) {
                long tierCountersAreaOffset = tierOffset + segmentHashLookupOuterSize;
                TierCountersArea.nextTierIndex(tierBytesStore.address(0) + tierCountersAreaOffset,
                        tierIndex + 1);
            }
        }

        // TODO HCOLL-397 insert msync here!

        // after we are sure the new bulk is initialized, update the global mutable state
        globalMutableState.setAllocatedExtraTierBulks(allocatedExtraTierBulks + 1);
        globalMutableState.setFirstFreeTierIndex(firstTierIndex);
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
            return msBytesSegmentOffset(tierIndexMinusOne);
        long extraTierIndex = tierIndexMinusOne - actualSegments;
        int bulkIndex = (int) (extraTierIndex >> log2TiersInBulk);
        if (bulkIndex >= tierBulkOffsets.size())
            mapTierBulks(bulkIndex);
        return tierBulkOffsets.get(bulkIndex).offset + tierBulkInnerOffsetToTiers +
                (extraTierIndex & (tiersInBulk - 1)) * segmentSize;
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
        return tierBulkData.bytesStore.address(0) + tierBulkData.offset +
                tierBulkInnerOffsetToTiers + tierIndexOffsetWithinBulk * segmentSize;
    }

    private void mapTierBulks(int upToBulkIndex) {
        if (persisted()) {
            try {
                mapTierBulksMapped(upToBulkIndex);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
        // TODO optimize -- create a file channel not on each tier bulk creation
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            // mapping by hand, because MappedFile/MappedBytesStore doesn't allow to create a BS
            // which starts not from the beginning of the file, but has start() of 0
            NativeBytesStore extraStore = map(raf, mapSize, mappingOffsetInFile);
            appendBulkData(firstBulkToMapIndex, upToBulkIndex, extraStore,
                    firstBulkToMapOffsetWithinMapping);
        }
    }

    private static NativeBytesStore map(
            RandomAccessFile raf, long mapSize, long mappingOffsetInFile) throws IOException {
        long address = OS.map(raf.getChannel(), READ_WRITE, mappingOffsetInFile, mapSize);
        OS.Unmapper unmapper = new OS.Unmapper(address, mapSize, DUMMY_REFERENCE_COUNTED);
        return new NativeBytesStore(address, mapSize, unmapper, false);
    }

    private long bulkOffset(int bulkIndex) {
        return sizeInBytesWithoutTiers() + bulkIndex * tierBulkSizeInBytes;
    }

    private void allocateTierBulks(int upToBulkIndex) {
        int firstBulkToMapIndex = tierBulkOffsets.size();
        int bulksToMap = upToBulkIndex + 1 - firstBulkToMapIndex;
        long mapSize = bulksToMap * tierBulkSizeInBytes;
        BytesStore extraStore = lazyNativeBytesStoreWithFixedCapacity(mapSize);
        appendBulkData(firstBulkToMapIndex, upToBulkIndex, extraStore, 0);
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
}
