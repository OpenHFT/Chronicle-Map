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

import net.openhft.chronicle.algo.locks.*;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytesStoreFactory;
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
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.util.stream.Collectors.toSet;
import static net.openhft.chronicle.algo.MemoryUnit.*;
import static net.openhft.chronicle.algo.bytes.Access.nativeAccess;
import static net.openhft.chronicle.bytes.NativeBytesStore.lazyNativeBytesStoreWithFixedCapacity;
import static net.openhft.chronicle.core.OS.pageAlign;
import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.*;
import static net.openhft.chronicle.hash.impl.DummyReferenceCounted.DUMMY_REFERENCE_COUNTED;

public abstract class VanillaChronicleHash<K,
        C extends HashEntry<K>, SC extends HashSegmentContext<K, ?>,
        ECQ extends ExternalHashQueryContext<K>>
        implements ChronicleHash<K, C, SC, ECQ>, Serializable, Marshallable {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleHash.class);

    private static final long serialVersionUID = 0L;

    public static final long TIER_COUNTERS_AREA_SIZE = 64;
    public static final long RESERVED_GLOBAL_MUTABLE_STATE_BYTES = 1024;

    /////////////////////////////////////////////////
    // Version
    private String dataFileVersion;

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
    // Checksum entries
    public boolean checksumEntries;

    /////////////////////////////////////////////////
    // Concurrency (number of segments), memory management and dependent fields
    public int actualSegments;
    public HashSplitting hashSplitting;

    public long entriesPerSegment;

    public long chunkSize;
    public int maxChunksPerEntry;
    public long actualChunksPerSegmentTier;

    /////////////////////////////////////////////////
    // Precomputed offsets and sizes for fast Context init
    int segmentHeaderSize;

    public int tierHashLookupValueBits;
    public int tierHashLookupKeyBits;
    public int tierHashLookupEntrySize;
    public long tierHashLookupCapacity;
    public long maxEntriesPerHashLookup;
    long tierHashLookupInnerSize;
    public long tierHashLookupOuterSize;

    public long tierFreeListInnerSize;
    public long tierFreeListOuterSize;

    long tierEntrySpaceInnerSize;
    public int tierEntrySpaceInnerOffset;
    long tierEntrySpaceOuterSize;

    public long tierSize;

    long maxExtraTiers;
    long tierBulkSizeInBytes;
    long tierBulkInnerOffsetToTiers;
    public long tiersInBulk;
    protected int log2TiersInBulk;

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

    protected transient volatile boolean closed;

    private transient VanillaGlobalMutableState globalMutableState;

    public VanillaChronicleHash(ChronicleMapBuilder<K, ?> builder) {
        createdOrInMemory = true;
        closed = false;

        // Version
        dataFileVersion = BuildVersion.version();

        @SuppressWarnings("deprecation")
        ChronicleHashBuilderPrivateAPI<K> privateAPI = builder.privateAPI();

        // Data model
        SerializationBuilder<K> keyBuilder = privateAPI.keyBuilder();
        keyClass = keyBuilder.tClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        keyReader = keyBuilder.reader();
        keyDataAccess = keyBuilder.dataAccess();

        actualSegments = privateAPI.actualSegments();
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);

        entriesPerSegment = privateAPI.entriesPerSegment();

        chunkSize = privateAPI.chunkSize();
        maxChunksPerEntry = privateAPI.maxChunksPerEntry();
        actualChunksPerSegmentTier = privateAPI.actualChunksPerSegmentTier();

        // Precomputed offsets and sizes for fast Context init
        segmentHeaderSize = privateAPI.segmentHeaderSize();

        tierHashLookupValueBits = valueBits(actualChunksPerSegmentTier);
        tierHashLookupKeyBits = keyBits(entriesPerSegment, tierHashLookupValueBits);
        tierHashLookupEntrySize =
                entrySize(tierHashLookupKeyBits, tierHashLookupValueBits);
        if (!privateAPI.aligned64BitMemoryOperationsAtomic() && tierHashLookupEntrySize > 4) {
            throw new IllegalStateException("aligned64BitMemoryOperationsAtomic() == false, " +
                    "but hash lookup slot is " + tierHashLookupEntrySize);
        }
        tierHashLookupCapacity = privateAPI.tierHashLookupCapacity();
        maxEntriesPerHashLookup = (long) (tierHashLookupCapacity * MAX_LOAD_FACTOR);
        tierHashLookupInnerSize = tierHashLookupCapacity * tierHashLookupEntrySize;
        tierHashLookupOuterSize = CACHE_LINES.align(tierHashLookupInnerSize, BYTES);

        tierFreeListInnerSize = LONGS.align(
                BYTES.alignAndConvert(actualChunksPerSegmentTier, BITS), BYTES);
        tierFreeListOuterSize = CACHE_LINES.align(tierFreeListInnerSize, BYTES);

        tierEntrySpaceInnerSize = chunkSize * actualChunksPerSegmentTier;
        tierEntrySpaceInnerOffset = privateAPI.segmentEntrySpaceInnerOffset();
        tierEntrySpaceOuterSize = CACHE_LINES.align(
                tierEntrySpaceInnerOffset + tierEntrySpaceInnerSize, BYTES);

        tierSize = segmentSize();

        maxExtraTiers = privateAPI.maxExtraTiers();
        tiersInBulk = computeNumberOfTiersInBulk();
        log2TiersInBulk = Maths.intLog2(tiersInBulk);
        tierBulkInnerOffsetToTiers = computeTierBulkInnerOffsetToTiers(tiersInBulk);
        tierBulkSizeInBytes = computeTierBulkBytesSize(tiersInBulk);

        checksumEntries = privateAPI.checksumEntries();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) {
        readMarshallableFields(wire);
        initTransients();
    }

    protected void readMarshallableFields(@NotNull WireIn wireIn) {
        // Previously assignment of these values was done in default field initializers, but
        // with Wire serialization VanillaChronicleMap instance is created with
        // unsafe.allocateInstance(), that doesn't guarantee (?) to initialize fields with default
        // values (false for boolean)
        createdOrInMemory = false;
        closed = false;

        dataFileVersion = wireIn.read(() -> "dataFileVersion").text();

        keyClass = wireIn.read(() -> "keyClass").typeLiteral();
        keySizeMarshaller = wireIn.read(() -> "keySizeMarshaller").typedMarshallable();
        keyReader = wireIn.read(() -> "keyReader").typedMarshallable();
        keyDataAccess = wireIn.read(() -> "keyDataAccess").typedMarshallable();

        checksumEntries = wireIn.read(() -> "checksumEntries").bool();

        actualSegments = wireIn.read(() -> "actualSegments").int32();
        hashSplitting = wireIn.read(() -> "hashSplitting").typedMarshallable();

        entriesPerSegment = wireIn.read(() -> "entriesPerSegment").int64();

        chunkSize = wireIn.read(() -> "chunkSize").int64();
        maxChunksPerEntry = wireIn.read(() -> "maxChunksPerEntry").int32();
        actualChunksPerSegmentTier = wireIn.read(() -> "actualChunksPerSegmentTier").int64();

        segmentHeaderSize = wireIn.read(() -> "segmentHeaderSize").int32();

        tierHashLookupValueBits = wireIn.read(() -> "tierHashLookupValueBits").int32();
        tierHashLookupKeyBits = wireIn.read(() -> "tierHashLookupKeyBits").int32();
        tierHashLookupEntrySize = wireIn.read(() -> "tierHashLookupEntrySize").int32();
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
        wireOut.write(() -> "keySizeMarshaller").typedMarshallable(keySizeMarshaller);
        wireOut.write(() -> "keyReader").typedMarshallable(keyReader);
        wireOut.write(() -> "keyDataAccess").typedMarshallable(keyDataAccess);

        wireOut.write(() -> "checksumEntries").bool(checksumEntries);

        wireOut.write(() -> "actualSegments").int32(actualSegments);
        wireOut.write(() -> "hashSplitting").typedMarshallable(hashSplitting);

        wireOut.write(() -> "entriesPerSegment").int64(entriesPerSegment);

        wireOut.write(() -> "chunkSize").int64(chunkSize);
        wireOut.write(() -> "maxChunksPerEntry").int32(maxChunksPerEntry);
        wireOut.write(() -> "actualChunksPerSegmentTier").int64(actualChunksPerSegmentTier);

        wireOut.write(() -> "segmentHeaderSize").int32(segmentHeaderSize);

        wireOut.write(() -> "tierHashLookupValueBits").int32(tierHashLookupValueBits);
        wireOut.write(() -> "tierHashLookupKeyBits").int32(tierHashLookupKeyBits);
        wireOut.write(() -> "tierHashLookupEntrySize").int32(tierHashLookupEntrySize);
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

    private long segmentSize() {
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
        while (tierSize * tiersInBulk < OS.pageSize()) {
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
        if (tierHashLookupEntrySize == 4) {
            hashLookup = new IntCompactOffHeapLinearHashTable(this);
        } else if (tierHashLookupEntrySize == 8) {
            hashLookup = new LongCompactOffHeapLinearHashTable(this);
        } else {
            throw new AssertionError("hash lookup slot size could be 4 or 8, " +
                    tierHashLookupEntrySize + " observed");
        }
    }

    public final void initBeforeMapping(File file, FileChannel ch, long headerSize)
            throws IOException {
        this.headerSize = roundUpMapHeaderSize(headerSize);
        if (!createdOrInMemory) {
            // This block is for reading segmentHeadersOffset before main mapping
            // After the mapping globalMutableState value's bytes are reassigned
            ByteBuffer globalMutableStateBuffer =
                    ByteBuffer.allocate((int) globalMutableState.maxSize());
            while (globalMutableStateBuffer.remaining() > 0) {
                if (ch.read(globalMutableStateBuffer,
                        this.headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET +
                                globalMutableStateBuffer.position()) == -1) {
                    throw new RuntimeException(file + " truncated");
                }
            }
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
        initBytesStoreAndHeadersViews(bytesStore);

        segmentHeadersOffset = segmentHeadersOffset();

        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;

        if (createdOrInMemory) {
            zeroOutNewlyMappedChronicleMapBytes();
            // write the segment headers offset after zeroing out
            globalMutableState.setSegmentHeadersOffset(segmentHeadersOffset);
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
        globalMutableState.bytesStore(bs, headerSize + GLOBAL_MUTABLE_STATE_VALUE_OFFSET,
                globalMutableState.maxSize());

        onHeaderCreated();
    }

    public final void createMappedStoreAndSegments(File file, RandomAccessFile raf)
            throws IOException {
        // TODO this method had been moved -- not clear where
        //OS.warnOnWindows(sizeInBytesWithoutTiers());
        this.file = file;
        long mapSize = expectedFileSize();
        createMappedStoreAndSegments(map(raf, mapSize, 0));
    }

    public final void basicRecover(File file, RandomAccessFile raf) throws IOException {
        this.file = file;
        long segmentHeadersOffset = computeSegmentHeadersOffset();
        long sizeInBytesWithoutTiers = computeSizeInBytesWithoutTiers(segmentHeadersOffset);
        long sizeBeyondSegments = Math.max(raf.length() - sizeInBytesWithoutTiers, 0);
        int allocatedExtraTierBulks = (int) (sizeBeyondSegments / tierBulkSizeInBytes);
        long mapSize = pageAlign(sizeInBytesWithoutTiers +
                allocatedExtraTierBulks * tierBulkSizeInBytes);
        initBytesStoreAndHeadersViews(map(raf, mapSize, 0));

        resetGlobalMutableStateLock(file);
        recoverAllocatedExtraTierBulks(file, allocatedExtraTierBulks);
        recoverSegmentHeadersOffset(file, segmentHeadersOffset);
        initBulks();
    }

    private void resetGlobalMutableStateLock(File file) {
        long lockAddr = globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET;
        LockingStrategy lockingStrategy = globalMutableStateLockingStrategy;
        long lockState = lockingStrategy.getState(nativeAccess(), null, lockAddr);
        if (lockState != lockingStrategy.resetState()) {
            LOG.error("global mutable state lock of map at {} is not clear: {}",
                    file, lockingStrategy.toString(lockState));
            lockingStrategy.reset(nativeAccess(), null, lockAddr);
        }
    }

    private void recoverAllocatedExtraTierBulks(File file, int allocatedExtraTierBulks) {
        if (globalMutableState.getAllocatedExtraTierBulks() != allocatedExtraTierBulks) {
            LOG.error("allocated extra tier bulks counter corrupted, or the map file {} " +
                    "is truncated. stored: {}, should be: {}", file,
                    globalMutableState.getAllocatedExtraTierBulks(), allocatedExtraTierBulks);
            globalMutableState.setAllocatedExtraTierBulks(allocatedExtraTierBulks);
        }
    }

    private void recoverSegmentHeadersOffset(File file, long segmentHeadersOffset) {
        if (globalMutableState.getSegmentHeadersOffset() != segmentHeadersOffset) {
            LOG.error("segment headers offset of map at {} corrupted. stored: {}, should be: {}",
                    file, globalMutableState.getSegmentHeadersOffset(), segmentHeadersOffset);
            globalMutableState.setSegmentHeadersOffset(segmentHeadersOffset);
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

    public final long expectedFileSize() {
        long sizeInBytesWithoutTiers = sizeInBytesWithoutTiers();
        int allocatedExtraTierBulks = !createdOrInMemory ?
                globalMutableState.getAllocatedExtraTierBulks() : 0;
        return pageAlign(sizeInBytesWithoutTiers + allocatedExtraTierBulks * tierBulkSizeInBytes);
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
        closed = true;
        if (file != null) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.getChannel().force(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        bs = null;
        file = null;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    public final void checkKey(Object key) {
        if (!keyClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException("Key must be a " + keyClass.getName() +
                    " but was a " + key.getClass());
        }
    }

    public final long segmentHeaderAddress(int segmentIndex) {
        return bsAddress() + segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    public long bsAddress() {
        return bs.address(0);
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
        // int division is MUCH faster than long on Intel CPUs
        sizeInBytes -= 1L;
        if (sizeInBytes <= Integer.MAX_VALUE)
            return (((int) sizeInBytes) / (int) chunkSize) + 1;
        return (int) (sizeInBytes / chunkSize) + 1;
    }

    @Override
    public final long longSize() {
        long result = 0L;
        for (int i = 0; i < segments(); i++) {
            try (SC c = segmentContext(i)) {
                result += c.size();
            }
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
                nativeAccess(), null,
                globalMutableStateAddress() + GLOBAL_MUTABLE_STATE_LOCK_OFFSET);
    }

    public void globalMutableStateUnlock() {
        globalMutableStateLockingStrategy.unlock(nativeAccess(), null,
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
                    long tierCountersAreaAddr = tierBaseAddr + tierHashLookupOuterSize;
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
        long firstTierIndex = extraTierIndexToTierIndex(allocatedExtraTierBulks * tiersInBulk);
        BytesStore tierBytesStore = tierBytesStore(firstTierIndex);
        long firstTierOffset = tierBytesOffset(firstTierIndex);
        if (tierBulkInnerOffsetToTiers > 0) {
            // These bytes are bit sets in Replicated version
            tierBytesStore.zeroOut(firstTierOffset - tierBulkInnerOffsetToTiers, firstTierOffset);
        }

        long lastTierIndex = firstTierIndex + tiersInBulk - 1;
        linkAndZeroOutFreeTiers(firstTierIndex, lastTierIndex);

        // TODO HCOLL-397 insert msync here!

        // after we are sure the new bulk is initialized, update the global mutable state
        globalMutableState.setAllocatedExtraTierBulks(allocatedExtraTierBulks + 1);
        globalMutableState.setFirstFreeTierIndex(firstTierIndex);
    }

    public void linkAndZeroOutFreeTiers(long firstTierIndex, long lastTierIndex) {
        for (long tierIndex = firstTierIndex; tierIndex <= lastTierIndex; tierIndex++) {
            long tierOffset = tierBytesOffset(tierIndex);
            BytesStore tierBytesStore = tierBytesStore(tierIndex);
            zeroOutNewlyMappedTier(tierBytesStore, tierOffset);
            if (tierIndex < lastTierIndex) {
                long tierCountersAreaOffset = tierOffset + tierHashLookupOuterSize;
                TierCountersArea.nextTierIndex(tierBytesStore.address(0) + tierCountersAreaOffset,
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
        return tierBulkData.bytesStore.address(0) + tierBulkData.offset +
                tierBulkInnerOffsetToTiers + tierIndexOffsetWithinBulk * tierSize;
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

    /**
     * @see net.openhft.chronicle.bytes.MappedFile#acquireByteStore(long, MappedBytesStoreFactory)
     */
    private static NativeBytesStore map(
            RandomAccessFile raf, long mapSize, long mappingOffsetInFile) throws IOException {
        long minFileSize = mappingOffsetInFile + mapSize;
        FileChannel fileChannel = raf.getChannel();
        if (fileChannel.size() < minFileSize) {
            // In MappedFile#acquireByteStore(), this is wrapped with fileLock(), to avoid race
            // condition between processes. This map() method is called either when a new tier is
            // allocated (in this case concurrent access is mutually excluded by
            // globalMutableStateLock), or on map creation, when race condition should be excluded
            // by self-bootstrapping header spec
            raf.setLength(minFileSize);
        }
        long address = OS.map(fileChannel, READ_WRITE, mappingOffsetInFile, mapSize);
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
