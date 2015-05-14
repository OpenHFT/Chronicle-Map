/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.KeyContext;
import net.openhft.chronicle.hash.impl.hashlookup.HashLookup;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.lang.Jvm;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.MappedStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.threadlocal.Provider;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static net.openhft.lang.MemoryUnit.*;

public abstract class VanillaChronicleHash<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        C extends KeyContext<K>> implements ChronicleHash<K, C>, Serializable {

    private static final long serialVersionUID = 0L;

    /////////////////////////////////////////////////
    // Version
    public final String dataFileVersion;

    /////////////////////////////////////////////////
    // Key Data model
    public final Class<K> kClass;
    public final SizeMarshaller keySizeMarshaller;
    public final BytesReader<K> originalKeyReader;
    public final KI originalKeyInterop;
    public final MKI originalMetaKeyInterop;
    public final MetaProvider<K, KI, MKI> metaKeyInteropProvider;

    public transient Provider<BytesReader<K>> keyReaderProvider;
    transient Provider<KI> keyInteropProvider;

    /////////////////////////////////////////////////
    // Concurrency (number of segments), memory management and dependent fields
    public final int actualSegments;
    final HashSplitting hashSplitting;

    public final long entriesPerSegment;

    public final long chunkSize;
    public final int maxChunksPerEntry;
    public final long actualChunksPerSegment;

    /////////////////////////////////////////////////
    // Precomputed offsets and sizes for fast Context init
    final int segmentHeaderSize;

    final int segmentHashLookupValueBits;
    final int segmentHashLookupKeyBits;
    final int segmentHashLookupEntrySize;
    final long segmentHashLookupCapacity;
    final long segmentHashLookupInnerSize;
    final long segmentHashLookupOuterSize;

    final long segmentFreeListInnerSize;
    final long segmentFreeListOuterSize;

    final long segmentEntrySpaceInnerSize;
    final int segmentEntrySpaceInnerOffset;
    final long segmentEntrySpaceOuterSize;

    final long segmentSize;

    /////////////////////////////////////////////////
    // Bytes Store (essentially, the base address) and serialization-dependent offsets
    public transient BytesStore ms;
    transient Bytes bytes;

    public transient long headerSize;
    transient long segmentHeadersOffset;
    transient long segmentsOffset;
    
    public VanillaChronicleHash(ChronicleHashBuilderImpl<K, ?, ?> builder, boolean replicated) {
        // Version
        dataFileVersion = BuildVersion.version();

        // Data model
        SerializationBuilder<K> keyBuilder = builder.keyBuilder();
        kClass = keyBuilder.eClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();
        originalKeyInterop = (KI) keyBuilder.interop();
        originalMetaKeyInterop = (MKI) keyBuilder.metaInterop();
        metaKeyInteropProvider = (MetaProvider<K, KI, MKI>) keyBuilder.metaInteropProvider();

        actualSegments = builder.actualSegments(replicated);
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);

        entriesPerSegment = builder.entriesPerSegment(replicated);

        chunkSize = builder.chunkSize(replicated);
        maxChunksPerEntry = builder.maxChunksPerEntry();
        actualChunksPerSegment = builder.actualChunksPerSegment(replicated);

        // Precomputed offsets and sizes for fast Context init
        segmentHeaderSize = builder.segmentHeaderSize(replicated);

        segmentHashLookupValueBits = HashLookup.valueBits(actualChunksPerSegment);
        segmentHashLookupKeyBits =
                HashLookup.keyBits(entriesPerSegment, segmentHashLookupValueBits);
        segmentHashLookupEntrySize =
                HashLookup.entrySize(segmentHashLookupKeyBits, segmentHashLookupValueBits);
        segmentHashLookupCapacity = HashLookup.capacityFor(entriesPerSegment);
        segmentHashLookupInnerSize = segmentHashLookupCapacity * segmentHashLookupEntrySize;
        segmentHashLookupOuterSize = CACHE_LINES.align(segmentHashLookupInnerSize, BYTES);

        segmentFreeListInnerSize = LONGS.align(
                BYTES.alignAndConvert(actualChunksPerSegment, BITS), BYTES);
        segmentFreeListOuterSize = CACHE_LINES.align(segmentFreeListInnerSize, BYTES);

        segmentEntrySpaceInnerSize = chunkSize * actualChunksPerSegment;
        segmentEntrySpaceInnerOffset = builder.segmentEntrySpaceInnerOffset(replicated);
        segmentEntrySpaceOuterSize = CACHE_LINES.align(
                segmentEntrySpaceInnerOffset + segmentEntrySpaceInnerSize, BYTES);

        segmentSize = segmentSize();
    }

    private long segmentSize() {
        long ss = segmentHashLookupOuterSize
                + segmentFreeListOuterSize
                + segmentEntrySpaceOuterSize;
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

    public void initTransients() {
        ownInitTransients();
    }

    private void ownInitTransients() {
        keyReaderProvider = Provider.of((Class) originalKeyReader.getClass());
        keyInteropProvider = Provider.of((Class) originalKeyInterop.getClass());
    }

    public final void createMappedStoreAndSegments(BytesStore bytesStore) throws IOException {
        this.ms = bytesStore;
        bytes = ms.bytes();

        onHeaderCreated();

        segmentHeadersOffset = mapHeaderOuterSize();
        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;
    }

    public void warnOnWindows() {
        if (!Jvm.isWindows())
            return;
        long offHeapMapSize = sizeInBytes();
        long oneGb = GIGABYTES.toBytes(1L);
        double offHeapMapSizeInGb = offHeapMapSize * 1.0 / oneGb;
        if (offHeapMapSize > GIGABYTES.toBytes(4L)) {
            System.out.printf(
                    "WARNING: On Windows, you probably cannot create a ChronicleMap\n" +
                            "of more than 4 GB. The configured map requires %.2f GB of " +
                            "off-heap memory.\n",
                    offHeapMapSizeInGb);
        }
        try {
            long freePhysicalMemory = Jvm.freePhysicalMemoryOnWindowsInBytes();
            if (offHeapMapSize > freePhysicalMemory * 0.9) {
                double freePhysicalMemoryInGb = freePhysicalMemory * 1.0 / oneGb;
                System.out.printf(
                        "WARNING: On Windows, you probably cannot create a ChronicleMap\n" +
                                "of more than 90%% of available free memory in the system.\n" +
                                "The configured map requires %.2f GB of off-heap memory.\n" +
                                "There is only %.2f GB of free physical memory in the system.\n",
                        offHeapMapSizeInGb, freePhysicalMemoryInGb);

            }
        } catch (IOException e) {
            // ignore -- anyway we just warn the user
        }
    }

    public final void createMappedStoreAndSegments(File file) throws IOException {
        warnOnWindows();
        createMappedStoreAndSegments(new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                sizeInBytes(), BytesMarshallableSerializer.create()));
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
        long pageMask = NativeBytes.UNSAFE.pageSize() - 1L;
        return (mapHeaderInnerSize() + pageMask) & ~pageMask;
    }

    public long mapHeaderInnerSize() {
        return headerSize;
    }

    @Override
    public File file() {
        return ms.file();
    }

    public final long sizeInBytes() {
        return mapHeaderOuterSize() + actualSegments * (segmentHeaderSize + segmentSize);
    }

    @Override
    public void close() {
        if (ms == null)
            return;
        bytes.release();
        bytes = null;
        ms.free();
        ms = null;
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
            sizes[i] = BigSegmentHeader.INSTANCE.size(ms.address() + segmentHeaderOffset(i));
        }
        return sizes;
    }

    public final long segmentHeaderOffset(int segmentIndex) {
        return segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    public final long segmentOffset(int segmentIndex) {
        return segmentsOffset + ((long) segmentIndex) * segmentSize;
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
            long segmentHeaderAddress = ms.address() + segmentHeaderOffset(i);
            result += BigSegmentHeader.INSTANCE.size(segmentHeaderAddress) -
                    BigSegmentHeader.INSTANCE.deleted(segmentHeaderAddress);
        }
        return result;
    }

    public final int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }
}
