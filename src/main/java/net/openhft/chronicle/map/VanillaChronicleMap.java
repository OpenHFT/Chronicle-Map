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
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.BytesBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.DelegatingMetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.map.VanillaContext.ContextFactory;
import net.openhft.lang.Jvm;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.MappedStore;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.FileChannel;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static net.openhft.chronicle.map.ChronicleMapBuilder.RUNTIME_PAGE_SIZE;
import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;
import static net.openhft.lang.MemoryUnit.*;

class VanillaChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>> extends AbstractChronicleMap<K, V>  {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleMap.class);

    private static final long serialVersionUID = 3L;

    /////////////////////////////////////////////////
    // Version
    final String dataFileVersion;

    /////////////////////////////////////////////////
    // Data model
    final Class<K> kClass;
    final SizeMarshaller keySizeMarshaller;
    final BytesReader<K> originalKeyReader;
    final KI originalKeyInterop;
    final MKI originalMetaKeyInterop;
    final MetaProvider<K, KI, MKI> metaKeyInteropProvider;

    final Class<V> vClass;
    final Class nativeValueClass;
    final SizeMarshaller valueSizeMarshaller;
    final BytesReader<V> originalValueReader;
    final VI originalValueInterop;
    final MVI originalMetaValueInterop;
    final MetaProvider<V, VI, MVI> metaValueInteropProvider;

    final DefaultValueProvider<K, V> defaultValueProvider;

    final boolean constantlySizedEntry;

    transient Provider<BytesReader<K>> keyReaderProvider;
    transient Provider<KI> keyInteropProvider;

    transient Provider<BytesReader<V>> valueReaderProvider;
    transient Provider<VI> valueInteropProvider;

    /////////////////////////////////////////////////
    // Event listener and meta data
    final int metaDataBytes;

    /////////////////////////////////////////////////
    // Behavior
    final boolean putReturnsNull;
    final boolean removeReturnsNull;

    /////////////////////////////////////////////////
    // Concurrency (number of segments), memory management and dependent fields
    final int actualSegments;
    final HashSplitting hashSplitting;

    final long entriesPerSegment;

    final long chunkSize;
    final int maxChunksPerEntry;
    final long actualChunksPerSegment;

    final Alignment alignment;
    final int worstAlignment;
    final boolean couldNotDetermineAlignmentBeforeAllocation;

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
    transient BytesStore ms;

    transient long headerSize;
    transient long segmentHeadersOffset;
    transient long segmentsOffset;

    /////////////////////////////////////////////////
    // Cached Entry Set instance


    public VanillaChronicleMap(ChronicleMapBuilder<K, V> builder) throws IOException {
        // Version
        dataFileVersion = BuildVersion.version();

        // Data model
        SerializationBuilder<K> keyBuilder = builder.keyBuilder;
        kClass = keyBuilder.eClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();
        originalKeyInterop = (KI) keyBuilder.interop();
        originalMetaKeyInterop = (MKI) keyBuilder.metaInterop();
        metaKeyInteropProvider = (MetaProvider<K, KI, MKI>) keyBuilder.metaInteropProvider();

        SerializationBuilder<V> valueBuilder = builder.valueBuilder;
        vClass = valueBuilder.eClass;
        if (vClass.getName().endsWith("$$Native")) {
            nativeValueClass = vClass;
        } else if (vClass.isInterface()) {
            Class nativeValueClass = null;
            try {
                nativeValueClass = DataValueClasses.directClassFor(vClass);
            } catch (Exception e) {
                // fall through
            }
            this.nativeValueClass = nativeValueClass;
        } else {
            nativeValueClass = null;
        }
        valueSizeMarshaller = valueBuilder.sizeMarshaller();
        originalValueReader = valueBuilder.reader();
        originalValueInterop = (VI) valueBuilder.interop();
        originalMetaValueInterop = (MVI) valueBuilder.metaInterop();
        metaValueInteropProvider = (MetaProvider) valueBuilder.metaInteropProvider();

        defaultValueProvider = builder.defaultValueProvider();

        constantlySizedEntry = builder.constantlySizedEntries();

        // Event listener and meta data
        metaDataBytes = builder.metaDataBytes();

        // Behavior
        putReturnsNull = builder.putReturnsNull();
        removeReturnsNull = builder.removeReturnsNull();

        // Concurrency (number of segments), memory management and dependent fields
        boolean replicated = getClass() == ReplicatedChronicleMap.class;
        actualSegments = builder.actualSegments(replicated);
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);

        entriesPerSegment = builder.entriesPerSegment(replicated);

        chunkSize = builder.chunkSize(replicated);
        maxChunksPerEntry = builder.maxChunksPerEntry();
        actualChunksPerSegment = builder.actualChunksPerSegment(replicated);

        alignment = builder.valueAlignment();
        worstAlignment = builder.worstAlignment(replicated);
        int alignment = this.alignment.alignment();
        couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;

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

        initTransients();
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

    void initTransients() {
        keyReaderProvider = Provider.of((Class) originalKeyReader.getClass());
        keyInteropProvider = Provider.of((Class) originalKeyInterop.getClass());

        valueReaderProvider = Provider.of((Class) originalValueReader.getClass());
        valueInteropProvider = Provider.of((Class) originalValueInterop.getClass());

        if (defaultValueProvider instanceof ConstantValueProvider) {
            ConstantValueProvider<K, V> constantValueProvider =
                    (ConstantValueProvider<K, V>) defaultValueProvider;
            if (constantValueProvider.wasDeserialized()) {
                ThreadLocalCopies copies = valueReaderProvider.getCopies(null);
                BytesReader<V> valueReader = valueReaderProvider.get(copies, originalValueReader);
                constantValueProvider.initTransients(valueReader);
            }
        }
    }

    final void createMappedStoreAndSegments(BytesStore bytesStore) throws IOException {
        this.ms = bytesStore;

        onHeaderCreated();

        segmentHeadersOffset = mapHeaderOuterSize();
        long segmentHeadersSize = actualSegments * segmentHeaderSize;
        segmentsOffset = segmentHeadersOffset + segmentHeadersSize;
    }

    void warnOnWindows() {
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

    final void createMappedStoreAndSegments(File file) throws IOException {
        warnOnWindows();
        createMappedStoreAndSegments(new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                sizeInBytes(), JDKObjectSerializer.INSTANCE));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
    }

    void onHeaderCreated() {
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
        long pageMask = RUNTIME_PAGE_SIZE - 1L;
        return (mapHeaderInnerSize() + pageMask) & ~pageMask;
    }

    long mapHeaderInnerSize() {
        return headerSize;
    }

    @Override
    public File file() {
        return ms.file();
    }

    final long sizeInBytes() {
        return mapHeaderOuterSize() + actualSegments * (segmentHeaderSize + segmentSize);
    }

    @Override
    public void close() {
        if (ms == null)
            return;
        ms.free();
        ms = null;
    }

    final void checkKey(Object key) {
        if (!kClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException("Key must be a " + kClass.getName() +
                    " but was a " + key.getClass());
        }
    }

    @Override
    final void checkValue(Object value) {
        if (vClass != Void.class && !vClass.isInstance(value)) {
            throw new ClassCastException("Value must be a " + vClass.getName() +
                    " but was a " + value.getClass());
        }
    }

    VanillaContext<K, KI, MKI, V, VI, MVI> mapContext() {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = rawContext();
        context.initMap(this);
        return context;
    }

    VanillaContext bytesMapContext() {
        VanillaContext context = rawBytesContext();
        context.initMap(this);
        return context;
    }

    @Override
    public VanillaContext<K, KI, MKI, V, VI, MVI> context(K key) {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = mapContext();
        context.initKey(key);
        return context;
    }

    @Override
    void putDefaultValue(VanillaContext context) {
        context.initPutDependencies();
        context.initInstanceValueModel0();
        context.initNewInstanceValue0(defaultValue(context));
        context.put0();
        context.closeValueModel();
        context.closeValue();
    }

    @Override
    int actualSegments() {
        return actualSegments;
    }

    @Override
    VanillaContext<K, KI, MKI, V, VI, MVI> rawContext() {
        return VanillaContext.get(VanillaContext.VanillaChronicleMapContextFactory.INSTANCE);
    }

    VanillaContext rawBytesContext() {
        return VanillaContext.get(BytesContextFactory.INSTANCE);
    }

    V defaultValue(KeyContext keyContext) {
        if (defaultValueProvider != null)
            return defaultValueProvider.get(keyContext);
        throw new IllegalStateException("To call acquire*() methods, " +
                "you should specify either default value or default value provider " +
                "during map building");
    }

    @Override
    V prevValueOnPut(MapKeyContext<K, V> context) {
        return putReturnsNull ? null : super.prevValueOnPut(context);
    }

    @Override
    V prevValueOnRemove(MapKeyContext<K, V> context) {
        return removeReturnsNull ? null : super.prevValueOnRemove(context);
    }

    @Override
    public V newValueInstance() {
        if (vClass == CharSequence.class || vClass == StringBuilder.class)
            return (V) new StringBuilder();
        return newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return newInstance(kClass, true);
    }

    @Override
    public Class<K> keyClass() {
        return kClass;
    }

    @Override
    public Class<V> valueClass() {
        return vClass;
    }

    static <T> T newInstance(Class<T> aClass, boolean isKey) {
        try {
            // TODO optimize -- save heap class / direct class in transient fields and call
            // newInstance() on them directly
            return isKey ? DataValueClasses.newInstance(aClass) :
                    DataValueClasses.newDirectInstance(aClass);

        } catch (Exception e) {
            if (e.getCause() instanceof IllegalStateException)
                throw e;

            if (aClass.isInterface())
                throw new IllegalStateException("It not possible to create a instance from " +
                        "interface=" + aClass.getSimpleName() + " we recommend you create an " +
                        "instance in the usual way.", e);

            try {
                return (T) aClass.newInstance();
            } catch (Exception e1) {
                throw new IllegalStateException("It has not been possible to create a instance " +
                        "of class=" + aClass.getSimpleName() +
                        ", Note : its more efficient if your chronicle map is configured with " +
                        "interface key " +
                        "and value types rather than classes, as this method is able to use " +
                        "interfaces to generate off heap proxies that point straight at your data. " +
                        "In this case you have used a class and chronicle is unable to create an " +
                        "instance of this class has it does not have a default constructor. " +
                        "If your class is mutable, we " +
                        "recommend you create and instance of your class=" + aClass.getSimpleName() +
                        " in the usual way, rather than using this method.", e);
            }
        }
    }


    @NotNull
    @Override
    public final MapKeyContext<K, V> acquireContext(K key, V usingValue) {
        VanillaContext<K, KI, MKI, V, VI, MVI> c = acquireContext(key);
        // TODO optimize to update lock in certain cases
        c.writeLock().lock();
        try {
            if (!c.containsKey()) {
                c.doPut(defaultValue(c));
                c.closeValue();
            }
            V value = c.getUsing(usingValue);
            if (value != usingValue) {
                throw new IllegalArgumentException("acquireContext MUST reuse the given " +
                        "value. Given value" + usingValue + " cannot be reused to read " + value);
            }
            return c;
        } catch (Throwable e) {
            try {
                c.closeMap();
            } catch (Throwable suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    VanillaContext<K, KI, MKI, V, VI, MVI> acquireContext(K key) {
        AcquireContext<K, KI, MKI, V, VI, MVI> c =
                VanillaContext.get(AcquireContextFactory.INSTANCE);
        c.initMap(this);
        c.initKey(key);
        return c;
    }

    static class AcquireContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends VanillaContext<K, KI, MKI, V, VI, MVI> {
        AcquireContext() {
            super();
        }

        AcquireContext(VanillaContext contextCache, int indexInContextCache) {
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

    enum AcquireContextFactory implements ContextFactory<AcquireContext> {
        INSTANCE;

        @Override
        public AcquireContext createContext(VanillaContext root,
                                                   int indexInContextCache) {
            return new AcquireContext(root, indexInContextCache);
        }

        @Override
        public AcquireContext createRootContext() {
            return new AcquireContext();
        }

        @Override
        public Class<AcquireContext> contextClass() {
            return AcquireContext.class;
        }
    }

    @Override
    public void clear() {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = mapContext();
        for (int i = 0; i < actualSegments; i++) {
            context.segmentIndex = i;
            try {
                context.clear();
            } finally {
                context.closeSegmentIndex();
            }
        }
    }

    @Override
    public final long longSize() {
        long result = 0L;
        for (int i = 0; i < actualSegments; i++) {
            result += BigSegmentHeader.INSTANCE.size(ms.address() + segmentHeaderOffset(i));
        }
        return result;
    }

    @Override
    public final int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    final long readValueSize(Bytes entry) {
        long valueSize = valueSizeMarshaller.readSize(entry);
        alignment.alignPositionAddr(entry);
        return valueSize;
    }

    static class BytesContext
            extends VanillaContext<Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>,
            Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>> {
        BytesContext() {
            super();
        }

        BytesContext(VanillaContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        @Override
        void initKeyModel0() {
            initBytesKeyModel0();
        }

        @Override
        void initKey0(Bytes key) {
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

    enum BytesContextFactory implements ContextFactory<BytesContext> {
        INSTANCE;

        @Override
        public BytesContext createContext(VanillaContext root, int indexInContextCache) {
            return new BytesContext(root, indexInContextCache);
        }

        @Override
        public BytesContext createRootContext() {
            return new BytesContext();
        }

        @Override
        public Class<BytesContext> contextClass() {
            return BytesContext.class;
        }
    }

    /**
     * For testing
     */
    final long[] segmentSizes() {
        long[] sizes = new long[actualSegments];
        for (int i = 0; i < actualSegments; i++) {
            sizes[i] = BigSegmentHeader.INSTANCE.size(ms.address() + segmentHeaderOffset(i));
        }
        return sizes;
    }

    final long segmentHeaderOffset(int segmentIndex) {
        return segmentHeadersOffset + ((long) segmentIndex) * segmentHeaderSize;
    }

    final long segmentOffset(int segmentIndex) {
        return segmentsOffset + ((long) segmentIndex) * segmentSize;
    }
}
