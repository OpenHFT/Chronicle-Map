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

import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.threadlocal.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.*;
import static net.openhft.lang.MemoryUnit.*;
import static net.openhft.lang.io.NativeBytes.UNSAFE;

public abstract class VanillaChronicleHash<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        C extends HashEntry<K>, SC extends HashSegmentContext<K, ?>,
        ECQ extends ExternalHashQueryContext<K>>
        implements ChronicleHash<K, C, SC, ECQ>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleHash.class);

    private static final long serialVersionUID = 0L;

    /////////////////////////////////////////////////
    // Version
    public final String dataFileVersion;

    /////////////////////////////////////////////////
    // If the hash was created in the first place, or read from disk
    public transient boolean created = false;

    /////////////////////////////////////////////////
    // Key Data model
    public final Class<K> kClass;
    public final SizeMarshaller keySizeMarshaller;
    public final BytesReader<K> originalKeyReader;
    public final KI originalKeyInterop;
    public final MKI originalMetaKeyInterop;
    public final MetaProvider<K, KI, MKI> metaKeyInteropProvider;

    public transient Provider<BytesReader<K>> keyReaderProvider;
    public transient Provider<KI> keyInteropProvider;

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
    protected final long numberOfTiersInBulk;
    protected final int log2NumberOfTiersInBulk;

    /////////////////////////////////////////////////
    // Bytes Store (essentially, the base address) and serialization-dependent offsets
    protected transient BytesStore ms;
    transient Bytes bytes;

    public static class TierBulkData {
        public BytesStore langBytesStore;
        public Bytes langBytes;
        public net.openhft.chronicle.bytes.BytesStore chBytesBytesStore;

        public long offset;

        public TierBulkData(BytesStore langBytesStore, long offset) {
            this.langBytesStore = langBytesStore;
            langBytes = langBytesStore.bytes();
            chBytesBytesStore = new NativeBytesStore<>(
                    langBytes.address(), langBytes.capacity(), null, false);
            this.offset = offset;
        }

        public TierBulkData(TierBulkData data, long offset) {
            this.langBytesStore = data.langBytesStore;
            this.langBytes = data.langBytes;
            this.chBytesBytesStore = data.chBytesBytesStore;
            this.offset = offset;
        }
    }

    public transient List<TierBulkData> tierBulkOffsets;

    public transient long headerSize;
    transient long segmentHeadersOffset;
    transient long segmentsOffset;

    public transient CompactOffHeapLinearHashTable hashLookup;

    protected transient boolean closed = false;

    public VanillaChronicleHash(
            ChronicleMapBuilder<K, ?> builder, boolean replicated) {
        // Version
        dataFileVersion = BuildVersion.version();

        // Because we are in constructor. If the Hash is not created, deserialization bypasses
        // the constructor and created = false
        this.created = true;

        @SuppressWarnings("deprecation")
        ChronicleHashBuilderPrivateAPI<K> privateAPI = builder.privateAPI();

        // Data model
        SerializationBuilder<K> keyBuilder = privateAPI.keyBuilder();
        kClass = keyBuilder.eClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();
        originalKeyInterop = (KI) keyBuilder.interop();
        originalMetaKeyInterop = (MKI) keyBuilder.metaInterop();
        metaKeyInteropProvider = (MetaProvider<K, KI, MKI>) keyBuilder.metaInteropProvider();

        actualSegments = privateAPI.actualSegments(replicated);
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);

        entriesPerSegment = privateAPI.entriesPerSegment(replicated);

        chunkSize = privateAPI.chunkSize(replicated);
        maxChunksPerEntry = privateAPI.maxChunksPerEntry(replicated);
        actualChunksPerSegment = privateAPI.actualChunksPerSegment(replicated);

        // Precomputed offsets and sizes for fast Context init
        segmentHeaderSize = privateAPI.segmentHeaderSize(replicated);

        segmentHashLookupValueBits = valueBits(actualChunksPerSegment);
        segmentHashLookupKeyBits = keyBits(entriesPerSegment, segmentHashLookupValueBits);
        segmentHashLookupEntrySize =
                entrySize(segmentHashLookupKeyBits, segmentHashLookupValueBits);
        segmentHashLookupCapacity = CompactOffHeapLinearHashTable.capacityFor(entriesPerSegment);
        segmentHashLookupInnerSize = segmentHashLookupCapacity * segmentHashLookupEntrySize;
        segmentHashLookupOuterSize = CACHE_LINES.align(segmentHashLookupInnerSize, BYTES);

        segmentFreeListInnerSize = LONGS.align(
                BYTES.alignAndConvert(actualChunksPerSegment, BITS), BYTES);
        segmentFreeListOuterSize = CACHE_LINES.align(segmentFreeListInnerSize, BYTES);

        segmentEntrySpaceInnerSize = chunkSize * actualChunksPerSegment;
        segmentEntrySpaceInnerOffset = privateAPI.segmentEntrySpaceInnerOffset(replicated);
        segmentEntrySpaceOuterSize = CACHE_LINES.align(
                segmentEntrySpaceInnerOffset + segmentEntrySpaceInnerSize, BYTES);

        segmentSize = segmentSize();

        maxExtraTiers = privateAPI.maxExtraTiers(replicated);
        numberOfTiersInBulk = computeNumberOfTiersInBulk();
        log2NumberOfTiersInBulk = Maths.intLog2(numberOfTiersInBulk);
        tierBulkInnerOffsetToTiers = computeTierBulkInnerOffsetToTiers(numberOfTiersInBulk);
        tierBulkSizeInBytes = computeTierBulkBytesSize();
    }

    private long segmentSize() {
        long ss = segmentHashLookupOuterSize + 64 /* Tier cache line */ +
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
        while (segmentSize * tiersInBulk < UNSAFE.pageSize()) {
            tiersInBulk *= 2;
        }
        return tiersInBulk;
    }

    private long computeTierBulkBytesSize() {
        return roundUpRuntimePageSize(tierBulkInnerOffsetToTiers +
                numberOfTiersInBulk * segmentSize);
    }

    protected long computeTierBulkInnerOffsetToTiers(long numberOfTiersInBulk) {
        return 0L;
    }

    public void initTransients() {
        ownInitTransients();
    }

    private void ownInitTransients() {
        keyReaderProvider = Provider.of((Class) originalKeyReader.getClass());
        keyInteropProvider = Provider.of((Class) originalKeyInterop.getClass());
        hashLookup = new CompactOffHeapLinearHashTable(this);
    }

    public final void createMappedStoreAndSegments(BytesStore bytesStore) throws IOException {
        this.ms = bytesStore;
        bytes = ms.bytes();

        onHeaderCreated();

        segmentHeadersOffset = mapHeaderOuterSize();
        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;

        initTierBulks(bytesStore.size());
    }

    public final void createMappedStoreAndSegments(File file) throws IOException {
        // TODO this method had been moved -- not clear where
        //OS.warnOnWindows(sizeInBytesWithoutTiers());
        createMappedStoreAndSegments(new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                // file.length() > sizeInBytesWithoutTiers() means there are some tiered segments
                Math.max(sizeInBytesWithoutTiers(), file.length()),
                BytesMarshallableSerializer.create()));
        // newly-extended file contents are not guaranteed to be zero
        if (created) {
            // TODO zero out only hash lookup tables and bit sets, not entry data areas
            bytes.zeroOut(headerSize, bytes.capacity(), true);
            // zero out tier data
            bytes.zeroOut(headerSize, headerSize + 64, true);
        }
    }

    private void initTierBulks(long byteStoreSize) {
        tierBulkOffsets = new ArrayList<>();
        long tierBulkOffset = roundUpRuntimePageSize(sizeInBytesWithoutTiers());
        if (byteStoreSize >= tierBulkOffset + tierBulkSizeInBytes) {
            TierBulkData firstTierBulkData = new TierBulkData(ms, tierBulkOffset);
            tierBulkOffsets.add(firstTierBulkData);
            while (true) {
                tierBulkOffset += tierBulkSizeInBytes;
                if (byteStoreSize < tierBulkOffset + tierBulkSizeInBytes)
                    break;
                tierBulkOffsets.add(new TierBulkData(firstTierBulkData, tierBulkOffset));
            }
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        ownInitTransients();
    }

    public void onHeaderCreated() {
    }

    /**
     * @return the version of Chronicle Map that was used to create the current data file
     */
    public String persistedDataVersion() {
        return dataFileVersion;
    }

    /**
     * @return the version of chronicle map that is currently running
     */
    public String applicationVersion() {
        return BuildVersion.version();
    }

    private long mapHeaderOuterSize() {
        // Align segment headers on page boundary to minimize number of pages that
        // segment headers span
        return roundUpRuntimePageSize(mapHeaderInnerSize());
    }

    private static long roundUpRuntimePageSize(long size) {
        long pageMask = UNSAFE.pageSize() - 1;
        return (size + pageMask) & ~pageMask;
    }

    public long mapHeaderInnerSize() {
        return headerSize + 64; /* tier data cache line */
    }

    @Override
    public File file() {
        return ms.file();
    }

    public final long sizeInBytesWithoutTiers() {
        return mapHeaderOuterSize() + actualSegments * (segmentHeaderSize + segmentSize);
    }

    public final long expectedFileSize() {
        long sizeInBytesWithoutTiers = sizeInBytesWithoutTiers();
        long extraAllocatedTierBulkCount = extraAllocatedTierBulkCount();
        if (extraAllocatedTierBulkCount == 0)
            return sizeInBytesWithoutTiers;
        return roundUpRuntimePageSize(sizeInBytesWithoutTiers) +
                extraAllocatedTierBulkCount * tierBulkSizeInBytes;
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
        if (ms == null)
            return;
        bytes.release();
        bytes = null;
        ms.free();
        ms = null;
        closed = true;
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
        return ms.address() + segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    public final long segmentBaseAddr(int segmentIndex) {
        return ms.address() + msBytesOffset(segmentIndex);
    }

    private long msBytesOffset(long segmentIndex) {
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

    private static final long TIER_DATA_LOCK_OFFSET = 0L;
    private static final long EXTRA_ALLOCATED_TIER_BULK_COUNT_OFFSET = TIER_DATA_LOCK_OFFSET + 8L;
    private static final long FIRST_FREE_TIER_INDEX_OFFSET =
            EXTRA_ALLOCATED_TIER_BULK_COUNT_OFFSET + 8L;
    private static final long TIERS_IN_USE_OFFSET = FIRST_FREE_TIER_INDEX_OFFSET + 8L;

    private long tierDataAddress() {
        return bytes.address() + headerSize;
    }

    private void tierDataLock() {
        // TODO this is quick access to locking functionality, eligible only because lock word
        // in the segment header has offset = 0
        BigSegmentHeader.INSTANCE.writeLock(tierDataAddress() + TIER_DATA_LOCK_OFFSET);
    }

    private void tierDataUnlock() {
        BigSegmentHeader.INSTANCE.writeUnlock(tierDataAddress() + TIER_DATA_LOCK_OFFSET);
    }

    protected long extraAllocatedTierBulkCount() {
        return bytes.readLong(headerSize + EXTRA_ALLOCATED_TIER_BULK_COUNT_OFFSET);
    }

    private void extraAllocatedTierBulkCount(long extraAllocatedTierBulkCount) {
        bytes.writeLong(headerSize + EXTRA_ALLOCATED_TIER_BULK_COUNT_OFFSET,
                extraAllocatedTierBulkCount);
    }

    private long firstFreeTierIndex() {
        return bytes.readLong(headerSize + FIRST_FREE_TIER_INDEX_OFFSET);
    }

    private void firstFreeTierIndex(long firstFreeTierIndex) {
        bytes.writeLong(headerSize + FIRST_FREE_TIER_INDEX_OFFSET, firstFreeTierIndex);
    }

    private long tiersInUse() {
        return bytes.readLong(headerSize + TIERS_IN_USE_OFFSET);
    }

    private void tiersInUse(long tiersInUse) {
        bytes.writeLong(headerSize + TIERS_IN_USE_OFFSET, tiersInUse);
    }

    public long allocateTier(int forSegmentIndex, int tier) {
        tierDataLock();
        try {
            long tiersInUse = tiersInUse();
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
                long firstFreeTierIndex = firstFreeTierIndex();
                if (firstFreeTierIndex > 0) {
                    tiersInUse(tiersInUse + 1);
                    long tierBaseAddr = tierIndexToBaseAddr(firstFreeTierIndex);
                    long tierDataAddr = tierBaseAddr + segmentHashLookupOuterSize;
                    long nextFreeTierIndex = TierData.nextTierIndex(tierDataAddr);
                    // now, when this tier will be a part of the map, the next tier designates
                    // the next tier in the data structure, should be 0
                    TierData.nextTierIndex(tierDataAddr, 0);
                    TierData.segmentIndex(tierDataAddr, forSegmentIndex);
                    TierData.tier(tierDataAddr, tier);
                    firstFreeTierIndex(nextFreeTierIndex);
                    return firstFreeTierIndex;
                } else {
                    allocateTierBulk();
                }
            }
        } finally {
            tierDataUnlock();
        }
    }

    private void allocateTierBulk() {
        long alreadyAllocatedBulks = extraAllocatedTierBulkCount();
        mapTiers(alreadyAllocatedBulks);
        extraAllocatedTierBulkCount(alreadyAllocatedBulks + 1);
        linkFreeTiersList(alreadyAllocatedBulks);
    }

    private void linkFreeTiersList(long alreadyAllocatedBulks) {
        long alreadyAllocatedTiers = alreadyAllocatedBulks * numberOfTiersInBulk;
        for (long tierIndex = alreadyAllocatedTiers;
             tierIndex < alreadyAllocatedTiers + numberOfTiersInBulk - 1; tierIndex++) {
            long tierBaseAddr = tierIndexToBaseAddr(tierIndex);
            long tierDataAddr = tierBaseAddr + segmentHashLookupOuterSize;
            TierData.nextTierIndex(tierDataAddr, tierIndex + 1);
        }
    }

    public long tierIndexToBaseAddr(long tierIndex) {
        // tiers are 1-counted, to allow tierIndex = 0 to be un-initialized in off-heap memory,
        // convert into 0-based form
        tierIndex -= 1;
        if (tierIndex < actualSegments)
            return segmentBaseAddr((int) tierIndex);
        return extraTierIndexToBaseAddress(tierIndex);
    }

    public Bytes tierBytes(long tierIndex) {
        tierIndex -= 1;
        if (tierIndex < actualSegments)
            return bytes;
        return tierBulkData(tierIndex).langBytes;
    }

    public long bytesOffset(long tierIndex) {
        tierIndex -= 1;
        if (tierIndex < actualSegments)
            return msBytesOffset(tierIndex);
        return tierBulkData(tierIndex).offset;
    }

    private TierBulkData tierBulkData(long tierIndex) {
        long extraTierIndex = tierIndex - actualSegments;
        long bulkIndex = extraTierIndex >> log2NumberOfTiersInBulk;
        if (bulkIndex >= tierBulkOffsets.size())
            mapTiers(bulkIndex);
        return tierBulkOffsets.get((int) bulkIndex);
    }

    private long extraTierIndexToBaseAddress(long tierIndex) {
        long extraTierIndex = tierIndex - actualSegments;
        long bulkIndex = extraTierIndex >> log2NumberOfTiersInBulk;
        if (bulkIndex >= tierBulkOffsets.size())
            mapTiers(bulkIndex);
        TierBulkData tierBulkData = tierBulkOffsets.get((int) bulkIndex);
        return tierBulkData.langBytes.address() + tierBulkData.offset +
                tierBulkInnerOffsetToTiers +
                (extraTierIndex & (numberOfTiersInBulk - 1)) * segmentSize;
    }

    private void mapTiers(long upToBulkIndex) {
        if (ms instanceof MappedStore) {
            try {
                mapTiersMapped(upToBulkIndex);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // in-memory ChMap
            allocateTiers(upToBulkIndex);
        }
    }

    private void mapTiersMapped(long upToBulkIndex) throws IOException {
        int firstBulkToMap = tierBulkOffsets.size();
        long bulksToMap = upToBulkIndex + 1 - firstBulkToMap;
        long mapSize = bulksToMap * tierBulkSizeInBytes;
        long mapStart = roundUpRuntimePageSize(sizeInBytesWithoutTiers()) +
                firstBulkToMap * tierBulkSizeInBytes;
        MappedStore extraStore = new MappedStore(file(), FileChannel.MapMode.READ_WRITE,
                mapStart, mapSize, ms.objectSerializer());
        appendTierBulkData(upToBulkIndex, firstBulkToMap, extraStore);
    }

    private void allocateTiers(long upToBulkIndex) {
        int firstBulkToMap = tierBulkOffsets.size();
        long bulksToMap = upToBulkIndex + 1 - firstBulkToMap;
        long mapSize = bulksToMap * tierBulkSizeInBytes;
        DirectStore extraStore = new DirectStore(ms.objectSerializer(), mapSize, true);
        appendTierBulkData(upToBulkIndex, firstBulkToMap, extraStore);
    }

    private void appendTierBulkData(long upToBulkIndex, int firstBulkToMap, BytesStore extraStore) {
        long offset = 0;
        TierBulkData firstMappedTierBulkData = new TierBulkData(extraStore, offset);
        tierBulkOffsets.add(firstMappedTierBulkData);
        for (long i = firstBulkToMap + 1; i <= upToBulkIndex; i++) {
            tierBulkOffsets.add(new TierBulkData(firstMappedTierBulkData,
                    offset += tierBulkSizeInBytes));
        }
    }
}
