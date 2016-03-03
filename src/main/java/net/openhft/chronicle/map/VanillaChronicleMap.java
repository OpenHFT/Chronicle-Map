/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashErrorListener;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.*;
import net.openhft.lang.Jvm;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.StatefulCopyable;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static net.openhft.chronicle.map.Asserts.assertNotNull;
import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;
import static net.openhft.lang.MemoryUnit.*;

class VanillaChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>> extends AbstractMap<K, V>
        implements ChronicleMap<K, V>, Serializable, ReadValue<V>, InstanceOrBytesToInstance,
        GetValueInterops<V, VI, MVI> {

    static final boolean checkSegmentMultiMapsAndBitSetsConsistencyOnBootstrap = Boolean.getBoolean(
            "chronicleMap.checkSegmentMultiMapsAndBitSetsConsistencyOnBootstrap");
    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleMap.class);
    private static final long serialVersionUID = 2L;
    final Class<K> kClass;
    final String dataFileVersion;
    final SizeMarshaller keySizeMarshaller;
    final BytesReader<K> originalKeyReader;
    final KI originalKeyInterop;
    final MKI originalMetaKeyInterop;
    final MetaProvider<K, KI, MKI> metaKeyInteropProvider;
    final Class<V> vClass;
    final SizeMarshaller valueSizeMarshaller;
    final BytesReader<V> originalValueReader;
    final VI originalValueInterop;
    final MVI originalMetaValueInterop;
    final MetaProvider<V, VI, MVI> metaValueInteropProvider;
    final DefaultValueProvider<K, V> defaultValueProvider;
    final PrepareValueBytesAsWriter<K> prepareValueBytesAsWriter;
    final int metaDataBytes;
    final long chunkSize;
    final Alignment alignment;
    final boolean constantlySizedEntry;
    final int worstAlignment;
    final boolean couldNotDetermineAlignmentBeforeAllocation;
    final int specialEntrySpaceOffset;
    final int actualSegments;
    final long actualChunksPerSegment;
    final long entriesPerSegment;
    final Class nativeValueClass;
    final MultiMapFactory multiMapFactory;
    final int maxChunksPerEntry;
    private final long lockTimeOutNS;
    private final int segmentHeaderSize;
    private final HashSplitting hashSplitting;
    private final String name;

    transient MapEventListener<K, V> eventListener;
    transient BytesMapEventListener bytesEventListener;
    // if set the ReturnsNull fields will cause some functions to return NULL
    // rather than as returning the Object can be expensive for something you probably don't use.
    transient boolean putReturnsNull;
    transient boolean removeReturnsNull;
    transient ChronicleHashErrorListener errorListener;

    transient Provider<BytesReader<K>> keyReaderProvider;
    transient Provider<KI> keyInteropProvider;
    transient Provider<BytesReader<V>> valueReaderProvider;
    transient Provider<VI> valueInteropProvider;
    transient Segment[] segments; // non-final for close()
    // non-final for close() and because it is initialized out of constructor
    transient BytesStore ms;
    transient long headerSize;
    transient Set<Map.Entry<K, V>> entrySet;
    transient InstanceOrBytesToInstance<Bytes, K> keyBytesToInstance;
    transient InstanceOrBytesToInstance<Bytes, V> valueBytesToInstance;
    transient InstanceOrBytesToInstance<Bytes, V> outputValueBytesToInstance;

    public VanillaChronicleMap(ChronicleMapBuilder<K, V> builder) {

        SerializationBuilder<K> keyBuilder = builder.keyBuilder;
        kClass = keyBuilder.eClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();
        dataFileVersion = BuildVersion.version();

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
        prepareValueBytesAsWriter = builder.prepareValueBytesAsWriter();

        lockTimeOutNS = builder.lockTimeOut(TimeUnit.NANOSECONDS);

        boolean replicated = getClass() == ReplicatedChronicleMap.class;
        this.chunkSize = builder.chunkSize(replicated);
        this.alignment = builder.valueAlignment();
        this.constantlySizedEntry = builder.constantlySizedEntries();
        this.worstAlignment = builder.worstAlignment(replicated);
        int alignment = this.alignment.alignment();
        this.couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;
        this.specialEntrySpaceOffset = builder.specialEntrySpaceOffset(replicated);

        this.putReturnsNull = builder.putReturnsNull();
        this.removeReturnsNull = builder.removeReturnsNull();

        this.actualSegments = builder.actualSegments(replicated);
        this.actualChunksPerSegment = builder.actualChunksPerSegment(replicated);
        this.entriesPerSegment = builder.entriesPerSegment(replicated);
        this.multiMapFactory = builder.multiMapFactory(replicated);
        this.metaDataBytes = builder.metaDataBytes();
        this.segmentHeaderSize = builder.segmentHeaderSize(replicated);
        this.maxChunksPerEntry = builder.maxChunksPerEntry();
        this.name = builder.name();
        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);
        initTransients(builder);
    }

    public String name() {
        return name;
    }

    static <T> T newInstance(Class<T> aClass, boolean isKey) {
        try {
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
                return aClass.newInstance();
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

    static void segmentStateNotNullImpliesCopiesNotNull(
            @Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState) {
        assert copies != null || segmentState == null; // segmentState != null -> copies != null
    }

    static void expectedValueNotNullImpliesBooleanResult(
            @Nullable Object expectedValue, boolean booleanResult) {
        assert booleanResult || expectedValue == null;
    }

    @Override
    public K readKey(Bytes entry, long keyPos) {
        long initialPos = entry.position();
        try {
            entry.position(keyPos);
            long keySize = keySizeMarshaller.readSize(entry);
            ThreadLocalCopies copies = keyReaderProvider.getCopies(null);
            return keyReaderProvider.get(copies, originalKeyReader).read(entry, keySize);
        } finally {
            entry.position(initialPos);
        }
    }

    @Override
    public V readValue(Bytes entry, long valuePos) {
        long initialPos = entry.position();
        try {
            entry.position(valuePos);
            long valueSize = valueSizeMarshaller.readSize(entry);
            alignment.alignPositionAddr(entry);
            ThreadLocalCopies copies = valueReaderProvider.getCopies(null);
            return valueReaderProvider.get(copies, originalValueReader).read(entry, valueSize);
        } finally {
            entry.position(initialPos);
        }
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

    final long segmentHash(long hash) {
        return hashSplitting.segmentHash(hash);
    }

    final int getSegment(long hash) {
        return hashSplitting.segmentIndex(hash);
    }

    void initTransients(ChronicleMapBuilder<K, V> builder) {
        if (builder != null) {
            this.eventListener = builder.eventListener();
            this.errorListener = builder.errorListener();
            this.putReturnsNull = builder.putReturnsNull();
            this.removeReturnsNull = builder.removeReturnsNull();
            this.bytesEventListener = builder.bytesEventListener();
        }

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

        @SuppressWarnings("unchecked")
        Segment[] ss = (Segment[]) Array.newInstance(segmentType(), actualSegments);
        this.segments = ss;

        keyBytesToInstance =
                new InstanceOrBytesToInstance<Bytes, K>() {
                    @Override
                    public K toInstance(@NotNull ThreadLocalCopies copies, Bytes keyBytes, long size) {
                        assertNotNull(copies);
                        BytesReader<K> keyReader = keyReaderProvider.get(copies, originalKeyReader);
                        return keyReader.read(keyBytes, size);
                    }
                };
        valueBytesToInstance =
                new InstanceOrBytesToInstance<Bytes, V>() {
                    @Override
                    public V toInstance(@NotNull ThreadLocalCopies copies, Bytes valueBytes, long size) {
                        return readValue(copies, valueBytes, null, size);
                    }
                };
        outputValueBytesToInstance =
                new InstanceOrBytesToInstance<Bytes, V>() {
                    @Override
                    public V toInstance(@NotNull ThreadLocalCopies copies, Bytes outputBytes, long size) {
                        outputBytes.position(outputBytes.position() - size);
                        return readValue(copies, outputBytes, null, size);
                    }
                };
    }

    Class segmentType() {
        return Segment.class;
    }

    final long createMappedStoreAndSegments(BytesStore bytesStore) {

        this.ms = bytesStore;
        onHeaderCreated();

        long offset = getHeaderSize();
        long segmentHeaderSize = getSegmentHeaderSize();
        long segmentHeadersSize = segments.length * segmentHeaderSize;
        Bytes segmentHeaders = ms.bytes(offset, segmentHeadersSize);
        offset += segmentHeadersSize;
        long segmentSize = segmentSize();
        long headerOffset = 0;
        for (int i = 0; i < this.segments.length; i++) {
            Bytes segmentHeader =
                    segmentHeaders.bytes(headerOffset, segmentHeaderSize);
            NativeBytes segmentData = (NativeBytes) ms.bytes(offset, segmentSize);
            this.segments[i] = createSegment(segmentHeader, segmentData, i);
            headerOffset += segmentHeaderSize;
            offset += segmentSize;
        }
        return offset;
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
                            "of more than 4 GB. The configured map requires %.2f GB of off-heap memory.\n",
                    offHeapMapSizeInGb);
        }
        if (Boolean.getBoolean("win.check"))
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

    final long createMappedStoreAndSegments(File file) throws IOException {
        warnOnWindows();
        return createMappedStoreAndSegments(new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                sizeInBytes(), JDKObjectSerializer.INSTANCE));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients(null);
    }

    /**
     * called when the header is created
     */
    void onHeaderCreated() {
    }

    long getHeaderSize() {
        return headerSize;
    }

    final long getSegmentHeaderSize() {
        return segmentHeaderSize;
    }

    Segment createSegment(Bytes segmentHeader, NativeBytes bytes, int index) {
        return new Segment(segmentHeader, bytes, index);
    }

    @Override
    public File file() {
        return ms.file();
    }

    final long sizeInBytes() {
        return getHeaderSize()
                + segments.length * getSegmentHeaderSize()
                + segments.length * segmentSize();
    }

    private long sizeOfMultiMap() {
        return multiMapFactory.sizeInBytes(entriesPerSegment);
    }

    long sizeOfMultiMapBitSet() {
        return MultiMapFactory.sizeOfBitSetInBytes(actualChunksPerSegment);
    }

    private long sizeOfSegmentFreeListBitSets() {
        return CACHE_LINES.align(BYTES.alignAndConvert(actualChunksPerSegment, BITS), BYTES);
    }

    private int numberOfBitSets() {
        return 1;
    }

    private long segmentSize() {
        long ss = (CACHE_LINES.align(sizeOfMultiMap() + sizeOfMultiMapBitSet(), BYTES)
                * multiMapsPerSegment())
                // the free list and 0+ dirty lists.
                + numberOfBitSets() * sizeOfSegmentFreeListBitSets()
                + sizeOfEntrySpaceInSegment();
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
            segmentSize |= 64L; // make segment size "odd" (in cache lines)
        }
        return segmentSize;
    }

    private int multiMapsPerSegment() {
        return 1;
    }

    private long sizeOfEntrySpaceInSegment() {
        return CACHE_LINES.align(specialEntrySpaceOffset + actualChunksPerSegment * chunkSize,
                BYTES);
    }

    @Override
    public void close() {
        if (ms == null)
            return;
        ms.free();
        segments = null;
        ms = null;
    }

    final void checkKey(Object key) {
        if (!kClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException("Key must be a " + kClass.getName() +
                    " but was a " + key.getClass());
        }
    }

    final void checkValue(Object value) {
        if (vClass != Void.class && !vClass.isInstance(value)) {
            throw new ClassCastException("Value must be a " + vClass.getName() +
                    " but was a " + value.getClass());
        }
    }

    final void put(Bytes entry, TcpReplicator.TcpSocketChannelEntryWriter output) {
        put(entry, output, true);
    }

    final void putIfAbsent(Bytes entry, TcpReplicator.TcpSocketChannelEntryWriter output) {
        put(entry, output, false);
    }

    private void put(Bytes entry, TcpReplicator.TcpSocketChannelEntryWriter output,
                     boolean replaceIfPresent) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        ReadValueToOutputBytes readValueToOutputBytes = segmentState.readValueToOutputBytes;
        readValueToOutputBytes.reuse(valueSizeMarshaller, output);
        long keySize = keySizeMarshaller.readSize(entry);
        entry.limit(entry.position() + keySize);
        GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                segmentState.getRemoteBytesValueInterops;
        MultiStoreBytes value = getRemoteBytesValueInterops.getValueBytes(
                entry, entry.position() + keySize);
        getRemoteBytesValueInterops.valueSizeMarshaller(valueSizeMarshaller);
        putBytes(copies, segmentState, entry, keySize, getRemoteBytesValueInterops, value,
                replaceIfPresent, readValueToOutputBytes);
    }

    void putBytes(ThreadLocalCopies copies, SegmentState segmentState, Bytes key, long keySize,
                  GetRemoteBytesValueInterops getRemoteBytesValueInterops, MultiStoreBytes value,
                  boolean replaceIfPresent, ReadValue<Bytes> readValue) {
        put2(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, key, keySize, keyBytesToInstance,
                getRemoteBytesValueInterops, value, valueBytesToInstance,
                replaceIfPresent, readValue, false);
    }

    final void put(Bytes entry) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        long keySize = keySizeMarshaller.readSize(entry);
        entry.limit(entry.position() + keySize);
        GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                segmentState.getRemoteBytesValueInterops;
        MultiStoreBytes value = getRemoteBytesValueInterops.getValueBytes(
                entry, entry.position() + keySize);
        getRemoteBytesValueInterops.valueSizeMarshaller(valueSizeMarshaller);
        ReadValueToBytes readValueToLazyBytes = segmentState.readValueToLazyBytes;
        readValueToLazyBytes.valueSizeMarshaller(valueSizeMarshaller);
        put2(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, entry, keySize, keyBytesToInstance,
                getRemoteBytesValueInterops, value, valueBytesToInstance,
                true, readValueToLazyBytes, true);
    }

    @Override
    public final V put(K key, V value) {
        return put1(key, value, true);
    }

    @Override
    public UpdateResult update(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();

        checkKey(key);
        checkValue(value);
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);

        return segments[segmentNum].update(copies, null,
                metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, value, valueIdentity(), segmentHash);
    }

    @Override
    public final V putIfAbsent(K key, V value) {
        if (key == null)
            throw new NullPointerException();
        return put1(key, value, false);
    }

    V put1(K key, V value, boolean replaceIfPresent) {
        if (key == null)
            throw new NullPointerException();
        checkKey(key);
        checkValue(value);
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        return put2(copies, null, metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, value, valueIdentity(), replaceIfPresent, this, putReturnsNull);
    }

    <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
            RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
    RV put2(ThreadLocalCopies copies, SegmentState segmentState,
            MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
            InstanceOrBytesToInstance<KB, K> toKey,
            GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
            InstanceOrBytesToInstance<? super VB, V> toValue,
            boolean replaceIfPresent, ReadValue<RV> readValue, boolean resultUnused) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);

        return segments[segmentNum].put3(copies, segmentState,
                metaKeyInterop, keyInterop, key, keySize, toKey,
                getValueInterops, value, toValue,
                segmentHash, replaceIfPresent, readValue, resultUnused);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final V get(Object key) {
        checkKey(key);
        return lookupUsing((K) key, null, false);
    }

    @Override
    public final V getUsing(K key, V usingValue) {
        checkKey(key);
        return lookupUsing(key, usingValue, false);
    }

    @Override
    public final V acquireUsing(@NotNull K key, V usingValue) {
        checkKey(key);
        return lookupUsing(key, usingValue, true);
    }

    final void getBytes(Bytes key, TcpReplicator.TcpSocketChannelEntryWriter output) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        ReadValueToOutputBytes readValueToOutputBytes = segmentState.readValueToOutputBytes;
        readValueToOutputBytes.reuse(valueSizeMarshaller, output);
        lookupUsing(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, key, keySizeMarshaller.readSize(key),
                keyBytesToInstance, readValueToOutputBytes, null, outputValueBytesToInstance,
                false, null);
    }

    private V lookupUsing(K key, V usingValue, boolean create) {
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        return lookupUsing(copies, null, metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, usingValue, valueIdentity(), create, null);
    }

    private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
    RV lookupUsing(ThreadLocalCopies copies, SegmentState segmentState,
                   MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                   InstanceOrBytesToInstance<KB, K> toKey,
                   ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                   boolean create, final MutableLockedEntry lock) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        Segment segment = segments[segmentNum];
        return segment.acquire(copies, segmentState,
                metaKeyInterop, keyInterop, key, keySize, toKey,
                readValue, usingValue, toValue, segmentHash, create, lock);
    }

    @Override
    public <R> R getMapped(K key, @NotNull Function<? super V, R> function) {
        try (ReadContext<K, V> entry = lookupUsing(key, null,
                false, false, LockType.READ_LOCK)) {
            return entry.present() ? function.apply(entry.value()) : null;
        }
    }

    @Override
    public V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {

        return (vClass.equals(CharSequence.class) || vClass.equals(StringBuilder.class))
                ? putMappedSB(key, unaryOperator)
                : putMapped0(key, unaryOperator);
    }

    V putMappedSB(K key, @NotNull UnaryOperator<V> unaryOperator) {

        StringBuilder usingSB = new StringBuilder("\uFFFF");
        try (WriteContext<K, V> entry = lookupUsing(key, (V) usingSB,
                false, true, LockType.WRITE_LOCK)) {

            usingSB = (StringBuilder) entry.value();
            if (usingSB.length() == 1 && usingSB.charAt(0) == '\uFFFF')
                return null;

            V result = unaryOperator.update((V) usingSB);
            if (usingSB != result) {
                usingSB.setLength(0);
                usingSB.append((CharSequence) result);
            }
            return result;
        }
    }

    V putMapped0(K key, @NotNull UnaryOperator<V> unaryOperator) {
        try (WriteContext<K, V> entry = lookupUsing(key, null,
                false, true, LockType.WRITE_LOCK)) {

            if (entry.value() == null)
                return null;

            V result = unaryOperator.update(entry.value());
            ((MutableLockedEntry) entry).value(result);
            return result;
        }
    }

    @Override
    public synchronized void getAll(File toFile) throws IOException {
        JsonSerializer.getAll(toFile, this, emptyList());
    }

    @Override
    public synchronized void putAll(File fromFile) throws IOException {
        JsonSerializer.putAll(fromFile, this, emptyList());
    }

    @Override
    public V newValueInstance() {
        if (vClass == CharSequence.class || vClass == StringBuilder.class) {
            return (V) new StringBuilder();
        }

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

    @NotNull
    @Override
    public final WriteContext<K, V> acquireUsingLocked(@NotNull K key, @NotNull V usingValue) {
        return lookupUsing(key, usingValue, true, true, LockType.WRITE_LOCK);
    }

    @NotNull
    @Override
    public final ReadContext<K, V> getUsingLocked(@NotNull K key, @NotNull V usingValue) {
        return lookupUsing(key, usingValue, true, false, LockType.READ_LOCK);
    }

    private <T extends Context> T lookupUsing(
            K key, V usingValue, boolean mustReuseValue, final boolean create, final LockType lockType) {
        checkKey(key);

        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);

        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        long hash = metaKeyInterop.hash(keyInterop, key);

        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);

        Segment segment = segments[segmentNum];
        boolean nativeValueClass = usingValue != null &&
                usingValue.getClass() == this.nativeValueClass;
        MutableLockedEntry<K, KI, MKI, V, VI, MVI> lock = (lockType == LockType.WRITE_LOCK)
                ? segment.writeLock(segmentState, nativeValueClass)
                : segment.readLock(segmentState);

        V v = segment.acquireWithoutLock(copies, segmentState,
                metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, usingValue, valueIdentity(),
                segmentHash, create, lock);

        checkReallyUsingValue(usingValue, mustReuseValue, v);

        lock.initKey(copies, metaKeyInterop, keyInterop, keySize, segmentHash, key);

        if (!nativeValueClass && lockType == LockType.WRITE_LOCK && v == null)
            v = usingValue;
        lock.value(v);

        return (T) lock;
    }

    private void checkReallyUsingValue(V usingValue, boolean mustReuseValue, V returnedValue) {
        if (mustReuseValue && returnedValue != null && returnedValue != usingValue)
            throw new IllegalArgumentException(
                    "acquireUsingLocked/getUsingLocked MUST reuse given " +
                            "values. Given value" + usingValue +
                            " cannot be reused to read " + returnedValue);
    }

    @Override
    public final Object toInstance(@NotNull ThreadLocalCopies copies, Object instance, long size) {
        return instance;
    }

    @SuppressWarnings("unchecked")
    final InstanceOrBytesToInstance<K, K> keyIdentity() {
        return this;
    }

    @SuppressWarnings("unchecked")
    final InstanceOrBytesToInstance<V, V> valueIdentity() {
        return this;
    }

    @Override
    public final boolean containsKey(final Object k) {
        checkKey(k);
        @SuppressWarnings("unchecked")
        K key = (K) k;
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        return containsKey(copies, metaKeyInterop, keyInterop, key,
                metaKeyInterop.size(keyInterop, key));
    }

    final boolean containsBytesKey(Bytes key) {
        return containsKey(null,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE,
                key, keySizeMarshaller.readSize(key));
    }

    private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>>
    boolean containsKey(ThreadLocalCopies copies,
                        MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        Segment segment = segments[segmentNum];
        return segment.containsKey(copies, metaKeyInterop, keyInterop, key, keySize, segmentHash);
    }

    @Override
    public void clear() {
        for (Segment segment : segments) {
            segment.clear();
        }
    }

    @NotNull
    @Override
    public final Set<Entry<K, V>> entrySet() {
        return (entrySet != null) ? entrySet : (entrySet = new EntrySet());
    }

    /**
     * @throws NullPointerException if the specified key is null
     */
    @Override
    @SuppressWarnings("unchecked")
    public final V remove(final Object key) {
        return (V) removeIfValueIs(key, null);
    }

    /**
     * @throws NullPointerException if the specified key is null
     */
    @Override
    @SuppressWarnings("unchecked")
    public final boolean remove(@net.openhft.lang.model.constraints.NotNull final Object key,
                                final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return (Boolean) removeIfValueIs(key, (V) value);
    }

    @Override
    public final VI getValueInterop(@NotNull ThreadLocalCopies copies) {
        assertNotNull(copies);
        return valueInteropProvider.get(copies, originalValueInterop);
    }

    @Override
    public final MVI getMetaValueInterop(
            @NotNull ThreadLocalCopies copies, VI valueInterop, V value) {
        assertNotNull(copies);
        return metaValueInteropProvider.get(
                copies, originalMetaValueInterop, valueInterop, value);
    }

    /**
     * removes ( if there exists ) an entry from the map, if the {@param key} and {@param
     * expectedValue} match that of a maps.entry. If the {@param expectedValue} equals null then (
     * if there exists ) an entry whose key equals {@param key} this is removed.
     *
     * @param k             the key of the entry to remove
     * @param expectedValue null if not required
     * @return true if and entry was removed
     */
    private Object removeIfValueIs(final Object k, final V expectedValue) {
        checkKey(k);
        @SuppressWarnings("unchecked")
        K key = (K) k;
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        return remove(copies, null, metaKeyInterop, keyInterop, key, keySize,
                keyIdentity(), this, expectedValue, valueIdentity(), this, removeReturnsNull);
    }

    final void removeBytesKeyWithoutOutput(Bytes key) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        ReadValueToBytes readValueToLazyBytes = segmentState.readValueToLazyBytes;
        readValueToLazyBytes.valueSizeMarshaller(valueSizeMarshaller);
        long keySize = keySizeMarshaller.readSize(key);
        key.limit(key.position() + keySize);
        remove(null, null, DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, key, keySize,
                keyBytesToInstance, null, null,
                outputValueBytesToInstance, readValueToLazyBytes, true);
    }

    final void removeBytesKeyOutputPrevValue(Bytes key,
                                             TcpReplicator.TcpSocketChannelEntryWriter output) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        ReadValueToOutputBytes readValueToOutputBytes = segmentState.readValueToOutputBytes;
        readValueToOutputBytes.reuse(valueSizeMarshaller, output);
        long keySize = keySizeMarshaller.readSize(key);
        key.limit(key.position() + keySize);
        remove(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, key, keySize, keyBytesToInstance, null, null,
                outputValueBytesToInstance, readValueToOutputBytes, false);
    }

    // for boolean remove(Object key, Object value);
    final boolean removeBytesEntry(Bytes entry) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        long keySize = keySizeMarshaller.readSize(entry);
        entry.limit(entry.position() + keySize);
        GetRemoteBytesValueInterops getRemoveBytesValueInterops =
                segmentState.getRemoteBytesValueInterops;
        MultiStoreBytes value = getRemoveBytesValueInterops.getValueBytes(
                entry, entry.position() + keySize);
        getRemoveBytesValueInterops.valueSizeMarshaller(valueSizeMarshaller);
        ReadValueToBytes readValueToLazyBytes = segmentState.readValueToLazyBytes;
        readValueToLazyBytes.valueSizeMarshaller(valueSizeMarshaller);
        return (Boolean) remove(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, entry, keySize, keyBytesToInstance,
                getRemoveBytesValueInterops, value, valueBytesToInstance,
                readValueToLazyBytes, false);
    }

    private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
            RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
    Object remove(ThreadLocalCopies copies, SegmentState segmentState,
                  MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                  InstanceOrBytesToInstance<KB, K> toKey,
                  GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                  InstanceOrBytesToInstance<RV, V> toValue,
                  ReadValue<RV> readValue, boolean resultUnused) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        Segment segment = segments[segmentNum];
        return segment.remove(copies, segmentState, metaKeyInterop, keyInterop, key, keySize,
                toKey, getValueInterops, expectedValue,
                toValue, segmentHash, readValue, resultUnused);
    }

    /**
     * @throws NullPointerException if any of the arguments are null
     */
    @Override
    public final boolean replace(@net.openhft.lang.model.constraints.NotNull K key,
                                 @net.openhft.lang.model.constraints.NotNull V oldValue,
                                 @net.openhft.lang.model.constraints.NotNull V newValue) {

        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();

        checkValue(oldValue);
        return (Boolean) replaceIfValueIs(key, oldValue, newValue);
    }

    /**
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    @SuppressWarnings("unchecked")
    public final V replace(@net.openhft.lang.model.constraints.NotNull final K key,
                           @net.openhft.lang.model.constraints.NotNull final V value) {
        if (key == null || value == null)
            throw new NullPointerException();

        return (V) replaceIfValueIs(key, null, value);
    }

    @Override
    public final long longSize() {
        long result = 0L;

        for (final Segment segment : this.segments) {
            result += segment.size();
        }

        return result;
    }

    @Override
    public final int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * replace the value in a map, only if the existing entry equals {@param expectedValue}
     *
     * @param key           the key into the map
     * @param expectedValue the expected existing value in the map ( could be null when we don't
     *                      wish to do this check )
     * @param newValue      the new value you wish to store in the map
     * @return the value that was replaced
     */
    private Object replaceIfValueIs(@net.openhft.lang.model.constraints.NotNull final K key,
                                    final V expectedValue, final V newValue) {
        checkKey(key);
        checkValue(newValue);
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        KI keyInterop = keyInteropProvider.get(copies, originalKeyInterop);
        copies = metaKeyInteropProvider.getCopies(copies);
        MKI metaKeyInterop =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyInterop, key);
        long keySize = metaKeyInterop.size(keyInterop, key);
        return replace(copies, null,
                metaKeyInterop, keyInterop, key, keySize, keyIdentity(),
                this, expectedValue, this, newValue, this, valueIdentity());
    }

    final void replaceKV(Bytes keyAndNewValue, TcpReplicator.TcpSocketChannelEntryWriter output) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);
        ReadValueToOutputBytes readValueToOutputBytes = segmentState.readValueToOutputBytes;
        readValueToOutputBytes.reuse(valueSizeMarshaller, output);

        long keySize = keySizeMarshaller.readSize(keyAndNewValue);
        keyAndNewValue.limit(keyAndNewValue.position() + keySize);
        GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                segmentState.getRemoteBytesValueInterops;
        MultiStoreBytes value = getRemoteBytesValueInterops.getValueBytes(
                keyAndNewValue, keyAndNewValue.position() + keySize);
        getRemoteBytesValueInterops.valueSizeMarshaller(valueSizeMarshaller);
        replace(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, keyAndNewValue, keySize, keyBytesToInstance,
                null, null, getRemoteBytesValueInterops, value,
                readValueToOutputBytes, valueBytesToInstance);
    }

    //  for boolean replace(K key, V oldValue, V newValue);
    final void replaceWithOldAndNew(Bytes entryAndNewValue, Bytes output) {
        ThreadLocalCopies copies = SegmentState.getCopies(null);
        SegmentState segmentState = SegmentState.get(copies);

        long keySize = keySizeMarshaller.readSize(entryAndNewValue);
        long keyPosition = entryAndNewValue.position();
        GetRemoteBytesValueInterops getExpectedValueInterops =
                segmentState.getRemoteBytesValueInterops;
        MultiStoreBytes expectedValue = getExpectedValueInterops.getValueBytes(
                entryAndNewValue, keyPosition + keySize);
        getExpectedValueInterops.valueSizeMarshaller(valueSizeMarshaller);

        entryAndNewValue.position(keyPosition + keySize);
        long expectedValueSize = valueSizeMarshaller.readSize(entryAndNewValue);
        GetRemoteBytesValueInterops getNewValueInterops = segmentState.getRemoteBytesValueInterops2;
        MultiStoreBytes newValue = getNewValueInterops.getValueBytes(entryAndNewValue,
                entryAndNewValue.position() + expectedValueSize);
        getNewValueInterops.valueSizeMarshaller(valueSizeMarshaller);

        entryAndNewValue.position(keyPosition);
        entryAndNewValue.limit(keyPosition + keySize);

        output.writeBoolean((Boolean) replace(copies, segmentState,
                DelegatingMetaBytesInterop.<Bytes, BytesInterop<Bytes>>instance(),
                BytesBytesInterop.INSTANCE, entryAndNewValue, keySize, keyBytesToInstance,
                getExpectedValueInterops, expectedValue, getNewValueInterops, newValue,
                null, valueBytesToInstance));
    }

    private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
            RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
    Object replace(
            ThreadLocalCopies copies, SegmentState segmentState,
            MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
            InstanceOrBytesToInstance<KB, K> toKey,
            GetValueInterops<VB, VBI, MVBI> getExpectedValueInterops, VB existingValue,
            GetValueInterops<VB, VBI, MVBI> getNewValueInterops, VB newValue,
            ReadValue<RV> readValue, InstanceOrBytesToInstance<? super RV, V> toValue) {
        long hash = metaKeyInterop.hash(keyInterop, key);
        int segmentNum = getSegment(hash);
        long segmentHash = segmentHash(hash);
        return segments[segmentNum].replace(copies, segmentState,
                metaKeyInterop, keyInterop, key, keySize, toKey,
                getExpectedValueInterops, existingValue, getNewValueInterops, newValue,
                readValue, toValue, segmentHash);
    }

    /**
     * For testing
     */
    final void checkConsistency() {
        for (Segment segment : segments) {
            segment.checkConsistency();
        }
    }

    final long readValueSize(Bytes entry) {
        long valueSize = valueSizeMarshaller.readSize(entry);
        alignment.alignPositionAddr(entry);
        return valueSize;
    }

    final V readValue(@NotNull ThreadLocalCopies copies, MultiStoreBytes entry, V value) {
        return readValue(copies, entry, value, readValueSize(entry));
    }

    @Override
    public final V readValue(
            @NotNull ThreadLocalCopies copies, Bytes entry, V value, long valueSize) {
        assertNotNull(copies);
        BytesReader<V> valueReader = valueReaderProvider.get(copies, originalValueReader);
        return valueReader.read(entry, valueSize, value);
    }

    @Override
    public final V readNull() {
        return null;
    }

    final void putAll(Bytes entries) {
        long numberOfEntries = entries.readStopBit();
        long entryPosition = entries.position();
        while (numberOfEntries-- > 0) {
            long keySize = keySizeMarshaller.readSize(entries);
            entries.skip(keySize);
            long valueSize = valueSizeMarshaller.readSize(entries);
            long nextEntryPosition = entries.position() + valueSize;
            entries.position(entryPosition);
            put(entries);
            entries.clear(); // because used as key, altering position and limit
            entryPosition = nextEntryPosition;
            entries.position(entryPosition);
        }
    }

    /**
     * For testing
     */
    final long[] segmentSizes() {
        long[] sizes = new long[segments.length];
        for (int i = 0; i < segments.length; i++)
            sizes[i] = segments[i].size();
        return sizes;
    }

    void onRemove(Segment segment, long pos) {
        // do nothing
    }

    void onRemoteRemove(Segment segment, long pos) {
        // do nothing
    }

    void onPut(Segment segment, long pos) {
        // do nothing
    }

    void onRemotePut(Segment segment, long pos) {
        // do nothing
    }

    void onRelocation(Segment segment, long pos) {
        // do nothing
    }

    void shouldNotBeCalledFromReplicatedChronicleMap(String method) {
        // do nothing
    }

    enum LockType {READ_LOCK, WRITE_LOCK}

    enum GetRemoteSeparateBytesInterops
            implements GetValueInterops<Bytes, BytesInterop<Bytes>,
            DelegatingMetaBytesInterop<Bytes, BytesInterop<Bytes>>> {
        INSTANCE;

        @Override
        public DelegatingMetaBytesInterop<Bytes, BytesInterop<Bytes>> getMetaValueInterop(
                @NotNull ThreadLocalCopies copies, BytesInterop<Bytes> valueInterop, Bytes value) {
            return DelegatingMetaBytesInterop.instance();
        }

        @Override
        public BytesInterop<Bytes> getValueInterop(@NotNull ThreadLocalCopies copies) {
            return BytesBytesInterop.INSTANCE;
        }
    }

    static abstract class ReadValueToBytes implements ReadValue<Bytes> {
        SizeMarshaller valueSizeMarshaller;

        abstract Bytes bytes(long valueSize);

        void valueSizeMarshaller(SizeMarshaller valueSizeMarshaller) {
            this.valueSizeMarshaller = valueSizeMarshaller;
        }

        @Override
        public Bytes readValue(@NotNull ThreadLocalCopies copies, Bytes entry, Bytes usingBytes,
                               long valueSize) {
            usingBytes = bytes(valueSize);
            valueSizeMarshaller.writeSize(usingBytes, valueSize);
            usingBytes.write(entry, entry.position(), valueSize);
            entry.skip(valueSize);
            return usingBytes;
        }
    }

    private static class ReadValueToOutputBytes extends ReadValueToBytes {
        private TcpReplicator.TcpSocketChannelEntryWriter output;

        void reuse(SizeMarshaller valueSizeMarshaller,
                   TcpReplicator.TcpSocketChannelEntryWriter output) {
            valueSizeMarshaller(valueSizeMarshaller);
            this.output = output;
        }

        @Override
        Bytes bytes(long valueSize) {
            return output.in();
        }

        @Override
        public Bytes readValue(@NotNull ThreadLocalCopies copies, Bytes entry, Bytes usingBytes,
                               long valueSize) {
            long totalSize = 1 + valueSizeMarshaller.sizeEncodingSize(valueSize) + valueSize;
            output.ensureBufferSize(totalSize);
            output.in().writeBoolean(false);
            return super.readValue(copies, entry, usingBytes, valueSize);
        }

        @Override
        public Bytes readNull() {
            output.ensureBufferSize(1);
            output.in().writeBoolean(true);
            return null;
        }
    }

    private static class ReadValueToLazyBytes extends ReadValueToBytes {
        DirectBytes lazyBytes;

        @Override
        Bytes bytes(long valueSize) {
            valueSize = valueSizeMarshaller.sizeEncodingSize(valueSize) + valueSize;
            if (lazyBytes != null) {
                if (lazyBytes.capacity() < valueSize) {
                    DirectStore store = (DirectStore) lazyBytes.store();
                    store.resize(valueSize, false);
                    lazyBytes = store.bytes();
                }
                lazyBytes.clear();
                return lazyBytes;
            }
            return lazyBytes = new DirectStore(valueSize).bytes();
        }

        @Override
        public Bytes readNull() {
            return null;
        }
    }

    static class GetRemoteBytesValueInterops
            implements GetValueInterops<MultiStoreBytes, Void, GetRemoteBytesValueInterops>,
            MetaBytesInterop<Bytes, Void> {
        private static final long serialVersionUID = 0L;
        private final MultiStoreBytes valueBytes = new MultiStoreBytes();

        private Bytes remoteEntryBytes;
        private long offset;
        private SizeMarshaller valueSizeMarshaller;

        private long valueSize;

        MultiStoreBytes getValueBytes(Bytes remoteEntryBytes, long offset) {
            this.remoteEntryBytes = remoteEntryBytes;
            this.offset = offset;
            this.valueSizeMarshaller = null;
            valueSize = -1L;
            return valueBytes;
        }

        void valueSize(long valueSize) {
            this.valueSize = valueSize;
        }

        void valueSizeMarshaller(SizeMarshaller valueSizeMarshaller) {
            this.valueSizeMarshaller = valueSizeMarshaller;
        }

        @Override
        public GetRemoteBytesValueInterops getMetaValueInterop(
                @NotNull ThreadLocalCopies copies, Void valueInterop, MultiStoreBytes value) {
            value.setBytesOffset(remoteEntryBytes, offset);
            if (valueSize == -1L)
                valueSize = valueSizeMarshaller.readSize(value);
            value.limit(value.position() + valueSize);
            return this;
        }

        @Override
        public Void getValueInterop(@NotNull ThreadLocalCopies copies) {
            return null;
        }

        @Override
        public boolean startsWith(Void interop, Bytes bytes, Bytes value) {
            return bytes.startsWith(value);
        }

        @Override
        public long hash(Void interop, Bytes value) {
            return BytesBytesInterop.INSTANCE.hash(value);
        }

        @Override
        public long size(Void writer, Bytes value) {
            return valueSize;
        }

        @Override
        public void write(Void writer, Bytes bytes, Bytes value) {
            bytes.write(value, value.position(), value.remaining());
        }
    }

    public static final class SegmentState implements StatefulCopyable<SegmentState>,
            AutoCloseable {
        private static final Provider<SegmentState> segmentStateProvider =
                Provider.of(SegmentState.class);
        private static final SegmentState originalSegmentState = new SegmentState(0);
        final MultiStoreBytes tmpBytes = new MultiStoreBytes();
        final GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                new GetRemoteBytesValueInterops();
        final GetRemoteBytesValueInterops getRemoteBytesValueInterops2 =
                new GetRemoteBytesValueInterops();
        final ReadValueToOutputBytes readValueToOutputBytes = new ReadValueToOutputBytes();
        final ReadValueToBytes readValueToLazyBytes = new ReadValueToLazyBytes();
        final SearchState searchState = new SearchState();
        // inner state
        private final int depth;
        // segment state (reusable objects/fields)
        private final ReadLocked readLocked = new ReadLocked(this);
        private final NativeWriteLocked nativeWriteLocked = new NativeWriteLocked(this);
        private final HeapWriteLocked heapWriteLocked = new HeapWriteLocked(this);
        long pos;
        long valueSizePos;
        byte identifier = 0;
        long timestamp = 0;
        private boolean used;
        private SegmentState next;

        private SegmentState(int depth) {
            if (depth > (1 << 10))
                throw new IllegalStateException("More than " + (1 << 10) +
                        " nested ChronicleMap contexts are not supported. Very probable that you " +
                        "simply forgot to close context somewhere (recommended to use " +
                        "try-with-resources statement). " +
                        "Otherwise this is a ChronicleMap bug, please report with this " +
                        "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues");
            this.depth = depth;
        }

        static
        @NotNull
        ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            return segmentStateProvider.getCopies(copies);
        }

        static
        @NotNull
        SegmentState get(@NotNull ThreadLocalCopies copies) {
            assertNotNull(copies);
            return segmentStateProvider.get(copies, originalSegmentState).get();
        }

        private SegmentState get() {
            if (!used) {
                used = true;
                return this;
            }
            SegmentState ss;
            for (ss = this; ss.next != null; ss = ss.next) {
                if (!ss.used) {
                    used = true;
                    return ss;
                }
            }
            return ss.next = new SegmentState(depth + 1);
        }

        @Override
        public Object stateIdentity() {
            return SegmentState.class;
        }

        @Override
        public SegmentState copy() {
            return new SegmentState(0);
        }

        @Override
        public void close() {
            used = false;
        }

        <K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
                V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        ReadLocked<K, KI, MKI, V, VI, MVI> readLocked(VanillaChronicleMap.Segment segment) {
            readLocked.initSegment(segment);
            initIdentifierAndTimestamp(segment);
            return readLocked;
        }

        private void initIdentifierAndTimestamp(VanillaChronicleMap.Segment segment) {
            VanillaChronicleMap map = segment.map();
            identifier = map instanceof ReplicatedChronicleMap ?
                    ((ReplicatedChronicleMap) map).identifier() : 0;
            timestamp = 0;
        }

        <K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
                V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        WriteLocked<K, KI, MKI, V, VI, MVI> nativeWriteLocked(VanillaChronicleMap.Segment segment) {
            nativeWriteLocked.initSegment(segment);
            initIdentifierAndTimestamp(segment);
            return nativeWriteLocked;
        }

        <K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
                V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        WriteLocked<K, KI, MKI, V, VI, MVI> heapWriteLocked(VanillaChronicleMap.Segment segment) {
            heapWriteLocked.initSegment(segment);
            initIdentifierAndTimestamp(segment);
            return heapWriteLocked;
        }
    }

    static abstract class MutableLockedEntry<
            K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>> implements Context<K, V> {
        final SegmentState segmentState;

        private VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map;
        private VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment;
        private ThreadLocalCopies copies;
        private MKI metaKeyInterop;
        private KI keyInterop;
        private long keySize;
        private long segmentHash;

        private K key;
        private V value;

        MutableLockedEntry(SegmentState segmentState) {
            this.segmentState = segmentState;
        }

        final void initSegment(VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment) {
            this.map = segment.map();
            this.segment = segment;
            init();
        }

        final void initKey(ThreadLocalCopies copies, MKI metaKeyInterop, KI keyInterop,
                           long keySize, long segmentHash, K key) {
            this.copies = copies;
            this.metaKeyInterop = metaKeyInterop;
            this.keyInterop = keyInterop;
            this.keySize = keySize;
            this.segmentHash = segmentHash;
            this.key = key;
            init();
        }

        void init() {
            // nothing
        }

        final VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map() {
            return map;
        }

        final VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment() {
            return segment;
        }

        final ThreadLocalCopies copies() {
            return copies;
        }

        final MKI metaKeyInterop() {
            return metaKeyInterop;
        }

        final KI keyInterop() {
            return keyInterop;
        }

        final long keySize() {
            return keySize;
        }

        final long segmentHash() {
            return segmentHash;
        }

        public final K key() {
            return key;
        }

        public final V value() {
            return value;
        }

        final void value(V value) {
            this.value = value;
        }

        @Override
        public void close() {
            segmentState.close();
            segment().readUnlock();
        }
    }

    private static class ReadLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends MutableLockedEntry<K, KI, MKI, V, VI, MVI>
            implements ReadContext<K, V> {

        ReadLocked(SegmentState segmentState) {
            super(segmentState);
        }

        /**
         * @return if the value is not null
         */
        public boolean present() {
            return value() != null;
        }
    }

    static abstract class WriteLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends MutableLockedEntry<K, KI, MKI, V, VI, MVI>
            implements WriteContext<K, V> {

        boolean removed;
        private boolean created;

        WriteLocked(SegmentState segmentState) {
            super(segmentState);
        }

        @Override
        void init() {
            super.init();
            created = false;
            removed = false;
        }

        /**
         * @return if the entry was created
         */
        public boolean created() {
            return created;
        }

        public void created(boolean created) {
            this.created = created;
        }
    }

    private static class NativeWriteLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends WriteLocked<K, KI, MKI, V, VI, MVI> {

        NativeWriteLocked(SegmentState segmentState) {
            super(segmentState);
        }

        @Override
        public void close() {
            if (!removed) {
                // TODO optimize -- keep replication bytes offset in SegmentState to jump directly
                long pos = segmentState.pos;
                VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment = segment();
                VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map = map();
                long offset = segment.offsetFromPos(pos);
                MultiStoreBytes entry = segment.reuse(segmentState.tmpBytes, offset);
                long keySize = map.keySizeMarshaller.readSize(entry);
                entry.skip(keySize);
                segment.manageReplicationBytes(segmentState, entry, true, false);
                map.onPut(segment, pos);
            }
            super.close();
        }

        @Override
        public void dontPutOnClose() {
            throw new IllegalStateException(
                    "dontPutOnClose() method is not supported for native value classes");
        }

        @Override
        public void removeEntry() {
            removed = true;
            VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map = map();
            segment().removeWithoutLock(copies(), segmentState,
                    metaKeyInterop(), keyInterop(), key(), keySize(), map.keyIdentity(),
                    map, null, map.valueIdentity(), segmentHash(), map, true);
        }
    }

    private static class HeapWriteLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends WriteLocked<K, KI, MKI, V, VI, MVI> {

        private boolean putOnClose;

        HeapWriteLocked(SegmentState segmentState) {
            super(segmentState);
        }

        @Override
        void init() {
            super.init();
            putOnClose = true;
        }

        @Override
        public void close() {
            VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment = segment();
            if (putOnClose) {
                assert !removed;
                // TODO optimize -- keep keySize, valueSizePos, entryEndAddr, etc. inside
                // segmentState
                long pos = segmentState.pos;
                VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map = map();
                ThreadLocalCopies copies = copies();
                long offset = segment.offsetFromPos(pos);
                MultiStoreBytes entry = segment.reuse(segmentState.tmpBytes, offset);
                long keySize = map.keySizeMarshaller.readSize(entry);
                entry.skip(keySize);
                segment.manageReplicationBytes(segmentState, entry, true, false);
                long valueSizePos = entry.position();
                long valueSize = map.valueSizeMarshaller.readSize(entry);
                long sizeOfEverythingBeforeValue = entry.position();
                map.alignment.alignPositionAddr(entry);
                long entryEndAddr = entry.positionAddr() + valueSize;
                VI valueInterop = map.valueInteropProvider.get(copies, map.originalValueInterop);
                V value = value();
                MVI metaValueInterop = map.metaValueInteropProvider
                        .get(copies, map.originalMetaValueInterop, valueInterop, value);
                long newValueSize = metaValueInterop.size(valueInterop, value);
                // doesn't call listeners?
                segment.putValue(pos, entry, valueSizePos, entryEndAddr, removed,
                        segmentState,
                        metaValueInterop, valueInterop, value, newValueSize, segment.hashLookup(),
                        sizeOfEverythingBeforeValue);
                map.onPut(segment, segmentState.pos);
            }
            super.close();
        }

        @Override
        public void dontPutOnClose() {
            if (removed)
                throw new IllegalStateException("Shouldn't call this method after removeEntry()");
            putOnClose = false;
        }

        @Override
        public void removeEntry() {
            putOnClose = false;
            removed = true;
            VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map = map();
            segment().removeWithoutLock(copies(), segmentState,
                    metaKeyInterop(), keyInterop(), key(), keySize(), map.keyIdentity(),
                    map, null, map.valueIdentity(), segmentHash(), map, true);
        }
    }

    // these methods should be package local, not public or private.
    class Segment implements SharedSegment {
        /*
        The entry format is
        - encoded length for key
        - bytes for the key
        - [possible alignment]
        - encoded length of the value
        - bytes for the value.
         */
        static final long LOCK_OFFSET = 0L; // 64-bit
        static final long SIZE_OFFSET = LOCK_OFFSET + 8L; // 32-bit
        final Bytes segmentHeader;
        final Bytes bytes;
        final long entriesOffset;
        private final int index, maxSize;
        private final SingleThreadedDirectBitSet freeList;
        long startWriteLock = 0;
        private MultiMap hashLookup;
        private long nextPosToSearchFrom = 0L;

        /**
         * @param index the index of this segment held by the map
         */
        Segment(Bytes segmentHeader, NativeBytes bytes, int index) {
            this.segmentHeader = segmentHeader;
            this.bytes = bytes;
            this.index = index;
            long start = bytes.startAddr();
            hashLookup = createMultiMap(start);
            maxSize = (int) (hashLookup.capacity() * 9 / 10);
            start += CACHE_LINES.align(sizeOfMultiMap() + sizeOfMultiMapBitSet(), BYTES)
                    * multiMapsPerSegment();
            final NativeBytes bsBytes = new NativeBytes(ms.objectSerializer(),
                    start,
                    start + MultiMapFactory.sizeOfBitSetInBytes(actualChunksPerSegment),
                    null);
            freeList = new SingleThreadedDirectBitSet(bsBytes);
            start += numberOfBitSets() * sizeOfSegmentFreeListBitSets();
            start += specialEntrySpaceOffset;
            entriesOffset = start - bytes.startAddr();
            assert bytes.capacity() >= entriesOffset + actualChunksPerSegment * chunkSize;
            if (checkSegmentMultiMapsAndBitSetsConsistencyOnBootstrap)
                checkMultiMapsAndBitSetsConsistency();
        }

        private VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map() {
            return VanillaChronicleMap.this;
        }

        final MultiMap hashLookup() {
            return hashLookup;
        }

        private MultiMap createMultiMap(long start) {
            final Bytes multiMapBytes =
                    new NativeBytes((ObjectSerializer) null, start,
                            start = start + sizeOfMultiMap(), null);

            final Bytes sizeOfMultiMapBitSetBytes =
                    new NativeBytes((ObjectSerializer) null, start,
                            start + sizeOfMultiMapBitSet(), null);
//            multiMapBytes.load();
            return multiMapFactory.create(multiMapBytes, sizeOfMultiMapBitSetBytes);
        }

        void checkMultiMapsAndBitSetsConsistency() {
            MultiMap hashLookup = hashLookup();
            final DirectBitSet positions = hashLookup.getPositions();
            class EntryChecker implements MultiMap.EntryConsumer {
                long size = 0;

                @Override
                public void accept(long key, long value) {
                    if (positions.isSet(value))
                        size++;
                    if (freeList.isClear(value))
                        throw new IllegalStateException("Position " + value + " is present in " +
                                "multiMap but available in the free chunk list");
                }
            }
            EntryChecker entryChecker = new EntryChecker();
            hashLookup.forEach(entryChecker);
            if (size() != entryChecker.size || entryChecker.size != positions.cardinality()) {
                throw new IllegalStateException("Segment inconsistent: " +
                        "size by Segment counter: " + size() +
                        ", size by multiMap records present in bit set: " + entryChecker.size +
                        ", size by multiMap bit set cardinality: " + positions.cardinality());
            }
        }

        /* Methods with private access modifier considered private to Segment
         * class, although Java allows to access them from outer class anyway.
         */
        public final int getIndex() {
            return index;
        }

        /**
         * increments the size by one
         */
        final void incrementSize() {
            this.segmentHeader.addLong(SIZE_OFFSET, 1L);
        }

        private void resetSize() {
            this.segmentHeader.writeLong(SIZE_OFFSET, 0L);
        }

        /**
         * decrements the size by one
         */
        final void decrementSize() {
            this.segmentHeader.addLong(SIZE_OFFSET, -1L);
        }

        /**
         * reads the the number of entries in this segment
         */
        long size() {
            // any negative value is in error state.
            return max(0L, this.segmentHeader.readVolatileLong(SIZE_OFFSET));
        }

        @Override
        public final ReadLocked<K, KI, MKI, V, VI, MVI> readLock(
                @Nullable SegmentState segmentState) {
            while (true) {
//                final boolean success = segmentHeader.tryRWReadLock(LOCK_OFFSET, lockTimeOutNS);
                boolean success = false;

                try {
                    success = segmentHeader.tryRWWriteLock(LOCK_OFFSET, lockTimeOutNS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

/*
                boolean success = false;
                try {
                    success = lock.readLock().tryLock(lockTimeOutNS, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
*/
                if (success) {
                    if (segmentState != null) {
                        return segmentState.readLocked(this);
                    } else {
                        return null;
                    }
                }
                if (currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            new InterruptedException("Unable to obtain lock, interrupted"));
                } else {
                    errorListener.onLockTimeout(segmentHeader.threadIdForLockLong(LOCK_OFFSET));
                    segmentHeader.resetLockLong(LOCK_OFFSET);
                }
            }
        }

        public WriteLocked<K, KI, MKI, V, VI, MVI> writeLock() {
            return writeLock(null, false);
        }

        final WriteLocked<K, KI, MKI, V, VI, MVI> writeLock(
                SegmentState segmentState, boolean nativeValueClass) {
            startWriteLock = System.nanoTime();
            while (true) {
                final boolean success;
                try {
                    success = segmentHeader.tryRWWriteLock(LOCK_OFFSET, lockTimeOutNS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
/*
                boolean success = false;
                try {
                    success = lock.writeLock().tryLock(lockTimeOutNS, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
*/
                if (success) {
                    if (segmentState != null) {
                        if (nativeValueClass) {
                            return segmentState.nativeWriteLocked(this);
                        } else {
                            return segmentState.heapWriteLocked(this);
                        }
                    } else {
                        return null;
                    }
                }

                if (currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            new InterruptedException("Unable to obtain lock, interrupted"));
                } else {
                    errorListener.onLockTimeout(segmentHeader.threadIdForLockLong(LOCK_OFFSET));
                    segmentHeader.resetLockLong(LOCK_OFFSET);
                }
            }
        }

        @Override
        public final void readUnlock() {
            try {
//                segmentHeader.unlockRWReadLock(LOCK_OFFSET);
                segmentHeader.unlockRWWriteLock(LOCK_OFFSET);
//                lock.readLock().unlock();
            } catch (IllegalMonitorStateException e) {
                errorListener.errorOnUnlock(e);
            }
        }

        @Override
        public final void writeUnlock() {
            try {
                segmentHeader.unlockRWWriteLock(LOCK_OFFSET);
//                lock.writeLock().unlock();
            } catch (IllegalMonitorStateException e) {
                errorListener.errorOnUnlock(e);
            }
            long lockTime = System.nanoTime() - startWriteLock;

            if (lockTime > 1e8 && LOG.isInfoEnabled())
                LOG.info("Thread took " + lockTime / 1000000 + "ms to release the lock, (Was there a GC?)");
        }

        @Override
        public final long offsetFromPos(long pos) {
            return entriesOffset + pos * chunkSize;
        }

        @Override
        public long timeStamp(long pos) {
            throw new UnsupportedOperationException("timeStamp are only supported by the " +
                    "replicated map");
        }

        final MultiStoreBytes reuse(MultiStoreBytes entry, long offset) {
            entry.setBytesOffset(bytes, offset);
            entry.position(metaDataBytes);
            return entry;
        }

        final MultiStoreBytes reuse2(MultiStoreBytes entry, long offset) {
            entry.setBytesOffset(bytes, offset);
            entry.position(metaDataBytes);
            if (bytes.readLong(offset) == 0)
                Thread.yield();
            return entry;
        }

        final long entrySize(long keySize, long valueSize) {
            long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
            return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
        }

        private long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
            if (constantlySizedEntry) {
                return alignment.alignAddr(sizeOfEverythingBeforeValue + valueSize);
            } else if (couldNotDetermineAlignmentBeforeAllocation) {
                return sizeOfEverythingBeforeValue + worstAlignment + valueSize;
            } else {
                return alignment.alignAddr(sizeOfEverythingBeforeValue) + valueSize;
            }
        }

        long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
            return metaDataBytes +
                    keySizeMarshaller.sizeEncodingSize(keySize) + keySize +
                    valueSizeMarshaller.sizeEncodingSize(valueSize);
        }

        final int inChunks(long sizeInBytes) {
            if (sizeInBytes <= chunkSize)
                return 1;
            // int division is MUCH faster than long on Intel CPUs
            sizeInBytes -= 1L;
            if (sizeInBytes <= Integer.MAX_VALUE)
                return (((int) sizeInBytes) / (int) chunkSize) + 1;
            return (int) (sizeInBytes / chunkSize) + 1;
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV acquire(@Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                   MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                   InstanceOrBytesToInstance<KB, K> toKey,
                   ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                   long hash2, boolean create, MutableLockedEntry lock) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            if (segmentState == null) {
                copies = SegmentState.getCopies(copies);
                segmentState = SegmentState.get(copies);
            }
            readLock(null);
            try {
                return acquireWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        readValue, usingValue, toValue, hash2, create, lock);
            } finally {
                segmentState.close();
                readUnlock();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV acquireWithoutLock(
                @NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, boolean create, MutableLockedEntry lock) {
            MultiStoreBytes entry = segmentState.tmpBytes;
            MultiMap hashLookup = this.hashLookup;
            SearchState searchState = segmentState.searchState;
            hashLookup.startSearch(hash2, searchState);
            for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                long offset = offsetFromPos(pos);
                reuse(entry, offset);
                if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                    continue;
                // key is found
                entry.skip(keySize);
                segmentState.pos = pos; // for WriteLocked.close()
                return readValueAndNotifyGet(copies, key, keySize, toKey,
                        readValue, usingValue, toValue, entry);
            }
            // key is not found
            if (!create)
                return readValue.readNull();

            RV result = createEntryOnAcquire(copies, segmentState,
                    metaKeyInterop, keyInterop, key, keySize, toKey,
                    readValue, usingValue, toValue, entry);
            entryCreated(lock);
            return result;
        }

        final void entryCreated(MutableLockedEntry lock) {
            //  notify the context that the entry was created
            if (lock instanceof WriteLocked)
                ((WriteLocked) lock).created(true);
        }

        final <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV createEntryOnAcquire(
                ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                MultiStoreBytes entry) {
            long valueSize;
            // omit copies assignment because it is 100% non-null (see above)
            // todo add api which doesn't require key instance for assigning default value bytes
            K keyInstance = toKey.toInstance(copies, key, keySize);
            if (defaultValueProvider != null) {
                V defaultValue = defaultValueProvider.get(keyInstance);
                VI valueInterop = valueInteropProvider.get(copies, originalValueInterop);
                MetaBytesWriter<V, ? super VI> metaValueInterop = metaValueInteropProvider.get(
                        copies, originalMetaValueInterop, valueInterop, defaultValue);

                valueSize = putEntry(segmentState,
                        metaKeyInterop, keyInterop, key, keySize,
                        metaValueInterop, valueInterop, defaultValue, entry, true);
            } else if (prepareValueBytesAsWriter != null) {
                valueSize = putEntry(segmentState,
                        metaKeyInterop, keyInterop, key, keySize,
                        prepareValueBytesAsWriter, null, keyInstance, entry, true);
            } else {
                throw defaultValueOrPrepareBytesShouldBeSpecified();
            }

            entry.positionAddr(entry.positionAddr() - valueSize);
            RV v = readValue.readValue(copies, entry, usingValue, valueSize);

            // put callbacks
            onPut(this, segmentState.pos);
            if (bytesEventListener != null) {
                long keyPos = metaDataBytes;
                byte replacedIdentifier = (byte) 0;
                long replacedTimeStamp = 0;
                bytesEventListener.onPut(entry, 0L, keyPos, segmentState.valueSizePos, true,
                        false, true,
                        segmentState.identifier, replacedIdentifier,
                        segmentState.timestamp, replacedTimeStamp, this);
            }
            if (eventListener != null && eventListener.isActive()) {
                byte replacedIdentifier = (byte) 0;
                long replacedTimeStamp = 0;
                final K key1 = toKey.toInstance(copies, key, keySize);
                final V newValue = eventListener.usesValue() ? toValue.toInstance(copies, v, valueSize) : null;
                writeUnlock();
                // TODO unlocking here is dangerous, because this method is a part of
                // acquireUsingLocked(), after which lock assumed to be valid
                try {
                    eventListener.onPut(key1,
                            newValue, null, false, true, true,
                            segmentState.identifier, replacedIdentifier,
                            segmentState.timestamp, replacedTimeStamp);
                } finally {
                    writeLock();
                }
            }

            return v;
        }

        final IllegalStateException defaultValueOrPrepareBytesShouldBeSpecified() {
            return new IllegalStateException("To call acquire*() methods, " +
                    "you should specify either default value, default value provider " +
                    "or prepareBytes strategy during map building");
        }

        final <KB, RV> RV readValueAndNotifyGet(
                ThreadLocalCopies copies,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                MultiStoreBytes entry) {
            long valueSize = readValueSize(entry);
            long valuePos = entry.position();
            RV v = readValue.readValue(copies, entry, usingValue, valueSize);

            // get callbacks
            if (bytesEventListener != null) {
                bytesEventListener.onGetFound(entry, 0L, metaDataBytes, valuePos);
            }
            if (eventListener != null && eventListener.isActive()) {
                eventListener.onGetFound(toKey.toInstance(copies, key, keySize),
                        eventListener.usesValue() ? toValue.toInstance(copies, v, valueSize) : null);
            }

            return v;
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        UpdateResult update(@Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                            MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                            InstanceOrBytesToInstance<KB, K> toKey,
                            GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                            InstanceOrBytesToInstance<? super VB, V> toValue,
                            long hash2) {
            shouldNotBeCalledFromReplicatedChronicleMap("Segment.update");
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            if (segmentState == null) {
                copies = SegmentState.getCopies(copies);
                segmentState = SegmentState.get(copies);
            }

            writeLock();
            try {
                SearchState searchState = segmentState.searchState;
                hashLookup.startSearch(hash2, searchState);
                MultiStoreBytes entry = segmentState.tmpBytes;
                for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);
                    VBI valueInterop = getValueInterops.getValueInterop(copies);
                    MVBI metaValueInterop = getValueInterops.getMetaValueInterop(
                            copies, valueInterop, value);
                    long valueSize = metaValueInterop.size(valueInterop, value);

                    long valueSizePos = entry.position();
                    long prevValueSize = valueSizeMarshaller.readSize(entry);
                    long sizeOfEverythingBeforeValue = entry.position();
                    alignment.alignPositionAddr(entry);

                    UpdateResult updateResult;
                    if (prevValueSize == valueSize &&
                            metaValueInterop.startsWith(valueInterop, entry, value)) {
                        updateResult = UpdateResult.UNCHANGED;
                        segmentState.pos = pos;
                    } else {
                        checkPreincrementSize();

                        long valueAddr = entry.positionAddr();
                        long entryEndAddr = valueAddr + prevValueSize;

                        // putValue may relocate entry and change offset
                        putValue(pos, entry, valueSizePos, entryEndAddr, false, segmentState,
                                metaValueInterop, valueInterop, value, valueSize, hashLookup,
                                sizeOfEverythingBeforeValue);

                        updateResult = UpdateResult.UPDATE;
                    }

                    // put callbacks
                    boolean hasValueChanged = updateResult != UpdateResult.UNCHANGED;
                    onPutMaybeRemote(segmentState.pos, false);
                    if (bytesEventListener != null)
                        bytesEventListener.onPut(entry, 0L, metaDataBytes, valueSizePos, false,
                                false, hasValueChanged,
                                segmentState.identifier, (byte) 0,
                                segmentState.timestamp, 0, this
                        );
                    if (eventListener != null && eventListener.isActive()) {
                        final K key1 = toKey.toInstance(copies, key, keySize);
                        final V newValue = eventListener.usesValue() ? toValue.toInstance(copies, value, valueSize) : null;
                        writeUnlock();
                        try {
                            eventListener.onPut(key1,
                                    newValue, null, false,
                                    false, hasValueChanged,
                                    segmentState.identifier, (byte) 0,
                                    segmentState.timestamp, 0);
                        } finally {
                            writeLock();
                        }
                    }

                    return updateResult;
                }
                // key is not found
                VBI valueInterop = getValueInterops.getValueInterop(copies);
                MVBI metaValueInterop =
                        getValueInterops.getMetaValueInterop(copies, valueInterop, value);
                long valueSize = putEntry(segmentState, metaKeyInterop, keyInterop, key, keySize,
                        metaValueInterop, valueInterop, value, entry, false);

                // put callbacks
                onPut(this, segmentState.pos);
                if (bytesEventListener != null) {
                    byte replacedIdentifier = 0;
                    long replacedTimeStamp = 0;
                    bytesEventListener.onPut(entry, 0L, metaDataBytes,
                            segmentState.valueSizePos, true, false, true,
                            segmentState.identifier, replacedIdentifier,
                            segmentState.timestamp, replacedTimeStamp, this);
                }
                if (eventListener != null && eventListener.isActive()) {
                    byte replacedIdentifier = 0;
                    long replacedTimeStamp = 0;
                    final K key1 = toKey.toInstance(copies, key, keySize);
                    final V newValue = eventListener.usesValue() ? toValue.toInstance(copies, value, valueSize) : null;

                    writeUnlock();
                    try {
                        eventListener.onPut(key1,
                                newValue, null, false, true, true,
                                segmentState.identifier, replacedIdentifier,
                                segmentState.timestamp, replacedTimeStamp);
                    } finally {
                        writeLock();
                    }
                }

                return UpdateResult.INSERT;
            } finally {
                segmentState.close();
                writeUnlock();
            }
        }

        protected void checkPreincrementSize() {
            long size = size();
            if (size > maxSize())
                throw new IllegalStateException("Segment contains " + size + " with maxSize: " + maxSize() + ", capacity: " + capacity());
            if (LOG.isDebugEnabled())
                LOG.debug("size: " + size + " of " + maxSize());
        }

        private long maxSize() {
            return maxSize;
        }

        private long capacity() {
            return entriesPerSegment;
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV put3(@Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                long hash2, boolean replaceIfPresent,
                ReadValue<RV> readValue, boolean resultUnused) {
            shouldNotBeCalledFromReplicatedChronicleMap("Segment.put");
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            if (segmentState == null) {
                copies = SegmentState.getCopies(copies);
                segmentState = SegmentState.get(copies);
            }
            writeLock();
            try {
                return putWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, value, toValue,
                        hash2, replaceIfPresent, readValue, resultUnused);
            } finally {
                segmentState.close();
                writeUnlock();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV putWithoutLock(
                @NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                long hash2, boolean replaceIfPresent,
                ReadValue<RV> readValue, boolean resultUnused) {

            SearchState searchState = segmentState.searchState;
            hashLookup.startSearch(hash2, searchState);
            MultiStoreBytes entry = segmentState.tmpBytes;
            for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                long offset = offsetFromPos(pos);
                reuse(entry, offset);
                if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                    continue;
                // key is found
                entry.skip(keySize);
                if (replaceIfPresent) {
                    return replaceValueAndNotifyPut(copies, segmentState,
                            key, keySize, toKey,
                            getValueInterops, value, toValue,
                            entry, pos, hashLookup, readValue, resultUnused,
                            false, false, (byte) 0, 0);
                } else {
                    long valueSize = readValueSize(entry);
                    return resultUnused ? null :
                            readValue.readValue(copies, entry, null, valueSize);
                }
            }

            // key is not found
            VBI valueInterop = getValueInterops.getValueInterop(copies);
            MVBI metaValueInterop =
                    getValueInterops.getMetaValueInterop(copies, valueInterop, value);

            long valueSize = putEntry(segmentState, metaKeyInterop, keyInterop, key, keySize,
                    metaValueInterop, valueInterop, value, entry, false);

            // put callbacks
            onPut(this, segmentState.pos);
            if (bytesEventListener != null)
                bytesEventListener.onPut(entry, 0L, metaDataBytes,
                        segmentState.valueSizePos, true, false, true,
                        segmentState.identifier, (byte) 0, segmentState.timestamp, 0, this);
            if (eventListener != null && eventListener.isActive()) {
                final K key1 = toKey.toInstance(copies, key, keySize);
                final V newValue = eventListener.usesValue() ? toValue.toInstance(copies, value, valueSize) : null;
                writeUnlock();
                try {
                    eventListener.onPut(key1,
                            newValue, null, false, true, true,
                            segmentState.identifier, (byte) 0, segmentState.timestamp, 0);
                } finally {
                    writeLock();
                }
            }

            return resultUnused ? null : readValue.readNull();
        }

        final <KB, RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV replaceValueAndNotifyPut(
                ThreadLocalCopies copies, SegmentState segmentState,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                MultiStoreBytes entry, long pos, MultiMap searchedHashLookup,
                ReadValue<RV> readValue, boolean resultUnused,
                boolean entryIsDeleted, boolean remote,
                byte replacedIdentifier, long replacedTimestamp) {
            VBI valueInterop = getValueInterops.getValueInterop(copies);
            MVBI metaValueInterop = getValueInterops.getMetaValueInterop(
                    copies, valueInterop, value);
            long valueSize = metaValueInterop.size(valueInterop, value);

            long valueSizePos = entry.position();
            long prevValueSize = valueSizeMarshaller.readSize(entry);
            long sizeOfEverythingBeforeValue = entry.position();
            alignment.alignPositionAddr(entry);
            long valueAddr = entry.positionAddr();
            long entryEndAddr = valueAddr + prevValueSize;

            RV prevValue = null;
            V prevValueInstance = null;
            if (!resultUnused && !entryIsDeleted)
                prevValue = readValue.readValue(copies, entry, null, prevValueSize);
            // todo optimize -- prevValue could be read twice
            if (eventListener != null && eventListener.isActive() && !entryIsDeleted) {
                entry.positionAddr(valueAddr);
                prevValueInstance = null;
                if (!putReturnsNull)
                    prevValueInstance = eventListener.usesValue() ? readValue(copies, entry, null, prevValueSize) : null;
            }

            boolean doPutValue;
            boolean hasValueChanged = false;
            if (eventListener != null && eventListener.isActive()) {
                entry.positionAddr(valueAddr);
                hasValueChanged = prevValueSize != valueSize ||
                        !metaValueInterop.startsWith(valueInterop, entry, value);
                doPutValue = hasValueChanged;
            } else {
                doPutValue = true;
            }

            if (doPutValue) {
                if (entryIsDeleted)
                    checkPreincrementSize();

                // putValue may relocate entry and change offset
                putValue(pos, entry, valueSizePos, entryEndAddr,
                        entryIsDeleted, segmentState,
                        metaValueInterop, valueInterop, value, valueSize, searchedHashLookup,
                        sizeOfEverythingBeforeValue);
            } else {
                segmentState.pos = pos;
            }

            replaceValueDeletedCallback(segmentState, searchedHashLookup, pos, entryIsDeleted);

            // put callbacks
            onPutMaybeRemote(segmentState.pos, remote);
            if (bytesEventListener != null)
                bytesEventListener.onPut(entry, 0L, metaDataBytes, valueSizePos, false, remote,
                        hasValueChanged,
                        segmentState.identifier, replacedIdentifier,
                        segmentState.timestamp, replacedTimestamp, this);
            if (eventListener != null && eventListener.isActive()) {
                final K key1 = toKey.toInstance(copies, key, keySize);
                final V newValue = eventListener.usesValue() ? toValue.toInstance(copies, value, valueSize) : null;

                writeUnlock();
                try {
                    eventListener.onPut(key1,
                            newValue, prevValueInstance, remote,
                            entryIsDeleted, hasValueChanged,
                            segmentState.identifier, replacedIdentifier,
                            segmentState.timestamp, replacedTimestamp);
                } finally {
                    writeLock();
                }
            }

            return resultUnused ? null : prevValue;
        }

        void replaceValueDeletedCallback(
                SegmentState segmentState, MultiMap hashLookup, long pos, boolean isDeleted) {
            if (isDeleted)
                throw new AssertionError();
            // do nothing
        }

        final void onPutMaybeRemote(long pos, boolean remote) {
            if (remote) {
                onRemotePut(this, pos);
            } else {
                onPut(this, pos);
            }
        }

        /**
         * Returns value size, writes the entry (key, value, sizes) to the entry, after this method
         * call entry positioned after value bytes written (i. e. at the end of entry), sets entry
         * position (in segment) and value size position in the given segmentState
         */
        final <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, E, EW>
        long putEntry(SegmentState segmentState,
                      MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                      MetaBytesWriter<E, ? super EW> metaElemWriter, EW elemWriter, E elem,
                      MultiStoreBytes entry, boolean writeDefaultInitialReplicationValues) {
            long valueSize = metaElemWriter.size(elemWriter, elem);
            long entrySize = entrySize(keySize, valueSize);
            int allocatedChunks = inChunks(entrySize);
            long pos = alloc(allocatedChunks);
            segmentState.pos = pos;
            long offset = offsetFromPos(pos);
            clearMetaData(offset);
            reuse(entry, offset);

            long position = entry.position();
            String addr = Long.toHexString(entry.address() + position);
            keySizeMarshaller.writeSize(entry, keySize);
            metaKeyInterop.write(keyInterop, entry, key);

            manageReplicationBytes(
                    segmentState, entry, writeDefaultInitialReplicationValues, false);

            segmentState.valueSizePos = entry.position();
            valueSizeMarshaller.writeSize(entry, valueSize);
            alignment.alignPositionAddr(entry);
            metaElemWriter.write(elemWriter, entry, elem);

            freeExtraAllocatedChunks(pos, allocatedChunks, entry);

            hashLookup.putAfterFailedSearch(segmentState.searchState, pos);
            incrementSize();

            long pos2 = entry.position();
            entry.position(position);
            if (LOG.isDebugEnabled())
                LOG.debug("write: " + addr + "\n" + entry.toHexString(32));
            entry.position(pos2);
            return valueSize;
        }

        final void freeExtraAllocatedChunks(long pos, int allocatedChunks, Bytes entry) {
            int actuallyUsedChunks;
            if (!constantlySizedEntry && couldNotDetermineAlignmentBeforeAllocation && // fast path
                    (actuallyUsedChunks = inChunks(entry.position())) < allocatedChunks) {
                free(pos + actuallyUsedChunks, allocatedChunks - actuallyUsedChunks);
            }
        }

        void manageReplicationBytes(
                SegmentState segmentState, Bytes entry,
                boolean writeDefaultInitialReplicationValues, boolean remove) {
            // do nothing
        }

        final void clearMetaData(long offset) {
            if (metaDataBytes > 0)
                bytes.zeroOut(offset, offset + metaDataBytes);
        }

        //TODO refactor/optimize
        final long alloc(int chunks) {
            if (chunks > maxChunksPerEntry)
                throw new IllegalArgumentException("Entry is too large: requires " + chunks +
                        " entry size chucks, " + maxChunksPerEntry + " is maximum.");
            long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom, chunks);
            if (ret == DirectBitSet.NOT_FOUND || ret + chunks > actualChunksPerSegment) {
                if (ret != DirectBitSet.NOT_FOUND &&
                        ret + chunks > actualChunksPerSegment && ret < actualChunksPerSegment)
                    freeList.clear(ret, actualChunksPerSegment);
                ret = freeList.setNextNContinuousClearBits(0L, chunks);
                if (ret == DirectBitSet.NOT_FOUND || ret + chunks > actualChunksPerSegment) {
                    if (ret != DirectBitSet.NOT_FOUND &&
                            ret + chunks > actualChunksPerSegment && ret < actualChunksPerSegment)
                        freeList.clear(ret, actualChunksPerSegment);
                    if (chunks == 1) {
                        throw new IllegalStateException(
                                "Segment is full, no free entries found");
                    } else {
                        throw new IllegalStateException(
                                "Segment is full or has no ranges of " + chunks
                                        + " continuous free chunks"
                        );
                    }
                }
                updateNextPosToSearchFrom(ret, chunks);
            } else {
                // if bit at nextPosToSearchFrom is clear, it was skipped because
                // more than 1 chunk was requested. Don't move nextPosToSearchFrom
                // in this case. chunks == 1 clause is just a fast path.
                if (chunks == 1 || freeList.isSet(nextPosToSearchFrom)) {
                    updateNextPosToSearchFrom(ret, chunks);
                }
            }
            return ret;
        }

        private void updateNextPosToSearchFrom(long allocated, int chunks) {
            if ((nextPosToSearchFrom = allocated + chunks) >= actualChunksPerSegment)
                nextPosToSearchFrom = 0L;
        }

        private boolean realloc(long fromPos, int oldChunks, int newChunks) {
            if (freeList.allClear(fromPos + oldChunks, fromPos + newChunks)) {
                freeList.set(fromPos + oldChunks, fromPos + newChunks);
                return true;
            } else {
                return false;
            }
        }

        private void free(long fromPos, int chunks) {
            freeList.clear(fromPos, fromPos + chunks);
            if (fromPos < nextPosToSearchFrom)
                nextPosToSearchFrom = fromPos;
        }

        final <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>>
        boolean keyEquals(KBI keyInterop, MKBI metaKeyInterop, KB key, long keySize, Bytes entry) {
            return keySize == keySizeMarshaller.readSize(entry) &&
                    metaKeyInterop.startsWith(keyInterop, entry, key);
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
        Object remove(@Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                      MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                      InstanceOrBytesToInstance<KB, K> toKey,
                      GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                      InstanceOrBytesToInstance<RV, V> toValue,
                      long hash2, ReadValue<RV> readValue, boolean resultUnused) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            if (segmentState == null) {
                copies = SegmentState.getCopies(copies);
                segmentState = SegmentState.get(copies);
            }
            writeLock();
            try {
                return removeWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, expectedValue, toValue,
                        hash2, readValue, resultUnused);
            } finally {
                segmentState.close();
                writeUnlock();
            }
        }

        /**
         * - if expectedValue is not null, returns Boolean.TRUE (removed) or Boolean.FALSE (entry
         * not found), regardless the expectedValue object is Bytes instance (RPC call) or the value
         * instance - if expectedValue is null: - if resultUnused is false, null or removed value is
         * returned - if resultUnused is true, null is always returned
         */
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<? super VB, ? super VBI>>
        Object removeWithoutLock(
                @NotNull ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, ReadValue<RV> readValue, boolean resultUnused) {
            MultiStoreBytes entry = segmentState.tmpBytes;
            SearchState searchState = segmentState.searchState;
            MultiMap hashLookup = hashLookup();
            hashLookup.startSearch(hash2, searchState);
            for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                long offset = offsetFromPos(pos);
                reuse(entry, offset);
                if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                    continue;
                // key is found
                entry.skip(keySize);
                long valueSizePos = entry.position();
                long valueSize = readValueSize(entry);

                // check the value assigned for the key is that we expect
                if (expectedValue != null) {
                    VBI valueInterop = getValueInterops.getValueInterop(copies);
                    MVBI metaValueInterop = getValueInterops.getMetaValueInterop(
                            copies, valueInterop, expectedValue);
                    if (metaValueInterop.size(valueInterop, expectedValue) != valueSize)
                        return Boolean.FALSE;
                    if (!metaValueInterop.startsWith(valueInterop, entry, expectedValue))
                        return Boolean.FALSE;
                }
                return removeEntry(copies, segmentState, key, keySize, toKey, toValue,
                        readValue, resultUnused, hashLookup, entry, pos, valueSizePos,
                        valueSize, false, true, expectedValue != null,
                        (byte) 0, 0);
            }
            // key is not found
            if (expectedValue == null) {
                return resultUnused ? null : readValue.readNull();
            } else {
                return Boolean.FALSE;
            }
        }

        final <KB, RV> Object removeEntry(
                ThreadLocalCopies copies, SegmentState segmentState,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                InstanceOrBytesToInstance<RV, V> toValue,
                ReadValue<RV> readValue, boolean resultUnused,
                MultiMap hashLookup, MultiStoreBytes entry, long pos,
                long valueSizePos, long valueSize, boolean remote, boolean removeFromMultiMap,
                boolean booleanResult, byte replacedIdentifier, long replacedTimestamp) {
            // get the removed value, if needed
            RV removedValue = null;
            if ((!booleanResult && !resultUnused) || (eventListener != null && eventListener.isActive())) {
                // todo reuse some value
                // todo check eventListener.usesValue()
                removedValue = readValue.readValue(copies, entry, null, valueSize);
                entry.position(entry.position() - valueSize);
            }

            // update segment state
            if (removeFromMultiMap) {
                hashLookup.removePrevPos(segmentState.searchState);
                long entrySizeInBytes = entry.positionAddr() + valueSize - entry.startAddr();
                free(pos, inChunks(entrySizeInBytes));
            } else {
                hashLookup.removePosition(pos);
            }
            decrementSize();

            // remove callbacks
            onRemoveMaybeRemote(pos, remote);
            if (bytesEventListener != null)
                bytesEventListener.onRemove(entry, 0L, metaDataBytes, valueSizePos, remote,
                        segmentState.identifier, replacedIdentifier,
                        segmentState.timestamp, replacedTimestamp, this);
            if (eventListener != null && eventListener.isActive()) {
                V removedValueForEventListener = eventListener.usesValue() ?
                        toValue.toInstance(copies, removedValue, valueSize) : null;

                final K key1 = toKey.toInstance(copies, key, keySize);
                writeUnlock();

                // TODO this is called from WriteContext.removeEntry(), losing exclusive lock
                // could be dangerous
                try {
                    eventListener.onRemove(key1,
                            removedValueForEventListener, remote,
                            segmentState.identifier, replacedIdentifier,
                            segmentState.timestamp, replacedTimestamp);
                } finally {
                    writeLock();
                }
            }

            return booleanResult ? Boolean.TRUE : removedValue;
        }

        final void onRemoveMaybeRemote(long pos, boolean remote) {
            if (remote) {
                onRemoteRemove(this, pos);
            } else {
                onRemove(this, pos);
            }
        }

        private <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>>
        boolean containsKey(ThreadLocalCopies copies,
                            MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize, long hash2) {
            readLock(null);
            copies = SegmentState.getCopies(copies);
            try (SegmentState segmentState = SegmentState.get(copies)) {
                MultiStoreBytes entry = segmentState.tmpBytes;
                MultiMap hashLookup = hashLookup();
                SearchState searchState = segmentState.searchState;
                hashLookup.startSearch(hash2, searchState);
                for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    if (isDeleted(entry, keySize))
                        continue;
                    return true;
                }
                return false;
            } finally {
                readUnlock();
            }
        }

        boolean isDeleted(Bytes entry, long keySize) {
            return false;
        }

        /**
         * Replaces the specified value for the key with the given value.  {@code newValue} is set
         * only if the existing value corresponding to the specified key is equal to {@code
         * expectedValue} or {@code expectedValue == null}.
         *
         * @param hash2 a hash code related to the {@code keyBytes}
         * @return the replaced value or {@code null} if the value was not replaced
         */
        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        Object replace(
                @Nullable ThreadLocalCopies copies, @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getExpectedValueInterops, VB expectedValue,
                GetValueInterops<VB, VBI, MVBI> getNewValueInterops, VB newValue,
                ReadValue<RV> readValue, InstanceOrBytesToInstance<? super RV, V> toValue,
                long hash2) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            if (segmentState == null) {
                copies = SegmentState.getCopies(copies);
                segmentState = SegmentState.get(copies);
            }
            writeLock();
            try {
                SearchState searchState = segmentState.searchState;
                hashLookup.startSearch(hash2, searchState);
                MultiStoreBytes entry = segmentState.tmpBytes;
                for (long pos; (pos = hashLookup.nextPos(searchState)) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    return onKeyPresentOnReplace(copies, segmentState, key, keySize, toKey,
                            getExpectedValueInterops, expectedValue, getNewValueInterops, newValue,
                            readValue, toValue, pos, entry, hashLookup, (byte) 0, 0);
                }
                // key is not found
                return expectedValue == null ? readValue.readNull() : Boolean.FALSE;
            } finally {
                segmentState.close();
                writeUnlock();
            }
        }

        final <KB, RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        Object onKeyPresentOnReplace(
                ThreadLocalCopies copies, @NotNull SegmentState segmentState,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getExpectedValueInterops, VB expectedValue,
                GetValueInterops<VB, VBI, MVBI> getNewValueInterops, VB newValue,
                ReadValue<RV> readValue, InstanceOrBytesToInstance<? super RV, V> toValue,
                long pos, MultiStoreBytes entry, MultiMap searchedHashLookup,
                byte replacedIdentifier, long replacedTimestamp) {
            long valueSizePos = entry.position();
            long valueSize = valueSizeMarshaller.readSize(entry);
            long sizeOfEverythingBeforeValue = entry.position();
            alignment.alignPositionAddr(entry);
            long valueAddr = entry.positionAddr();
            long entryEndAddr = valueAddr + valueSize;
            RV prevValue;

            if (expectedValue != null) {
                // check the value assigned for the key is that we expect
                VBI valueInterop = getExpectedValueInterops.getValueInterop(copies);
                MVBI metaValueInterop = getExpectedValueInterops.getMetaValueInterop(
                        copies, valueInterop, expectedValue);
                if (metaValueInterop.size(valueInterop, expectedValue) != valueSize)
                    return Boolean.FALSE;
                if (!metaValueInterop.startsWith(valueInterop, entry, expectedValue))
                    return Boolean.FALSE;
                prevValue = expectedValue;
            } else {
                // todo reuse value
                prevValue = readValue.readValue(copies, entry, null, valueSize);
            }

            VBI valueInterop = getNewValueInterops.getValueInterop(copies);
            MVBI metaValueInterop = getNewValueInterops.getMetaValueInterop(
                    copies, valueInterop, newValue);
            long newValueSize = metaValueInterop.size(valueInterop, newValue);
            boolean doPutValue;
            boolean hasValueChanged = false;
            if (eventListener != null && eventListener.isActive()) {
                entry.positionAddr(valueAddr);
                hasValueChanged = valueSize != newValueSize ||
                        !metaValueInterop.startsWith(valueInterop, entry, newValue);
                doPutValue = hasValueChanged;
            } else {
                doPutValue = true;
            }
            if (doPutValue) {
                checkPreincrementSize();
                boolean entryIsDeleted = false; // couldn't replace deleted entry
                putValue(pos, entry, valueSizePos, entryEndAddr, entryIsDeleted, segmentState,
                        metaValueInterop, valueInterop, newValue, newValueSize, searchedHashLookup,
                        sizeOfEverythingBeforeValue);
            } else {
                segmentState.pos = pos;
            }

            updateReplicationBytesOnKeyPresentOnReplace(entry,
                    valueSizePos - ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES,
                    segmentState.timestamp, segmentState.identifier);

            // put callbacks
            onPut(this, segmentState.pos);
            if (bytesEventListener != null) {
                long keyPos = metaDataBytes;
                bytesEventListener.onPut(entry, 0L, keyPos, segmentState.valueSizePos, true,
                        false, hasValueChanged,
                        segmentState.identifier, replacedIdentifier,
                        segmentState.timestamp, replacedTimestamp, this);

            }
            if (eventListener != null && eventListener.isActive()) {
                final K key1 = toKey.toInstance(copies, key, keySize);
                final V newValue1 = eventListener.usesValue() ? toValue.toInstance(copies, newValue, newValueSize) : null;
                final V replacedValue = eventListener.usesValue() ? toValue.toInstance(copies, prevValue, valueSize) : null;
                writeUnlock();
                try {
                    eventListener.onPut(key1,
                            newValue1,
                            replacedValue, false, true,
                            hasValueChanged,
                            segmentState.identifier, replacedIdentifier,
                            segmentState.timestamp, replacedTimestamp);
                } finally {
                    writeLock();
                }
            }

            return expectedValue == null ? prevValue : Boolean.TRUE;
        }

        void updateReplicationBytesOnKeyPresentOnReplace(
                Bytes entry, long timestampPos, long timestamp, byte identifier) {
            // do nothing
        }

        /**
         * Replaces value in existing entry. May cause entry relocation, because there may be not
         * enough space for new value in location already allocated for this entry.
         *
         * @param pos          index of the first chunk occupied by the entry
         * @param entry        relative pointer in Segment bytes
         * @param valueSizePos relative position of value size in entry
         * @param entryEndAddr absolute address of the entry end
         */
        final <E, EW> void putValue(
                long pos, MultiStoreBytes entry, long valueSizePos, long entryEndAddr,
                boolean entryIsDeleted,
                SegmentState segmentState,
                MetaBytesWriter<E, ? super EW> metaElemWriter, EW elemWriter, E newElem,
                long newElemSize,
                MultiMap searchedHashLookup, long sizeOfEverythingBeforeValue) {
            long entryStartAddr = entry.address();
            long valueSizeAddr = entryStartAddr + valueSizePos;
            long newValueAddr = alignment.alignAddr(
                    valueSizeAddr + valueSizeMarshaller.sizeEncodingSize(newElemSize));
            long newEntryEndAddr = newValueAddr + newElemSize;
            newValueDoesNotFit:
            if (newEntryEndAddr != entryEndAddr) {
                long oldEntrySize = entryEndAddr - entryStartAddr;
                int oldSizeInChunks = inChunks(oldEntrySize);
                int newSizeInChunks = inChunks(newEntryEndAddr - entryStartAddr);
                if (newSizeInChunks > oldSizeInChunks) {
                    if (newSizeInChunks > maxChunksPerEntry) {
                        throw new IllegalArgumentException("Value too large: " +
                                "entry takes " + newSizeInChunks + " chunks, " +
                                maxChunksPerEntry + " is maximum.");
                    }
                    if (realloc(pos, oldSizeInChunks, newSizeInChunks))
                        break newValueDoesNotFit;
                    // RELOCATION
                    onRelocation(this, pos);
                    int allocatedChunks =
                            inChunks(innerEntrySize(sizeOfEverythingBeforeValue, newElemSize));
                    long newPos = alloc(allocatedChunks);
                    // free after new alloc, to avoid overlapping allocation => undef. behaviour
                    // on unsafe.copyMemory
                    free(pos, oldSizeInChunks);
                    // putValue() is called from put() and replace()
                    // after successful search by key
                    searchedHashLookup.replacePrevPos(segmentState.searchState, newPos,
                            !entryIsDeleted);
                    long newOffset = offsetFromPos(newPos);
                    reuse(entry, newOffset);
                    // Moving metadata, key size and key.
                    // Don't want to fiddle with pseudo-buffers for this,
                    // since we already have all absolute addresses.
                    long newEntryStartAddr = entry.address();
                    NativeBytes.UNSAFE.copyMemory(entryStartAddr,
                            newEntryStartAddr, valueSizeAddr - entryStartAddr);
                    entry.position(valueSizePos);
                    valueSizeMarshaller.writeSize(entry, newElemSize);
                    alignment.alignPositionAddr(entry);
                    metaElemWriter.write(elemWriter, entry, newElem);
                    freeExtraAllocatedChunks(newPos, allocatedChunks, entry);
                    segmentState.pos = newPos;
                    return;
                    // END OF RELOCATION
                } else if (newSizeInChunks < oldSizeInChunks) {
                    // Freeing extra chunks
                    freeList.clear(pos + newSizeInChunks, pos + oldSizeInChunks);
                    // Do NOT reset nextPosToSearchFrom, because if value
                    // once was larger it could easily became larger again,
                    // But if these chunks will be taken by that time,
                    // this entry will need to be relocated.
                }
            }
            // Common code for all cases
            entry.position(valueSizePos);
            valueSizeMarshaller.writeSize(entry, newElemSize);
            alignment.alignPositionAddr(entry);
            metaElemWriter.write(elemWriter, entry, newElem);
            segmentState.pos = pos;
        }

        private void clear() {
            shouldNotBeCalledFromReplicatedChronicleMap("Segment.clear");
            writeLock();
            try {
                hashLookup.clear();
                freeList.clear();
                nextPosToSearchFrom = 0L;
                resetSize();
            } finally {
                writeUnlock();
            }
        }

        public Entry<K, V> getEntry(@NotNull SegmentState segmentState, long pos) {
            Bytes entry = reuse(segmentState.tmpBytes, offsetFromPos(pos));

            long keySize = keySizeMarshaller.readSize(entry);
            ThreadLocalCopies copies = keyReaderProvider.getCopies(null);
            K key = keyReaderProvider.get(copies, originalKeyReader).read(entry, keySize);

            long valueSize = valueSizeMarshaller.readSize(entry);
            alignment.alignPositionAddr(entry);
            copies = valueReaderProvider.getCopies(copies);
            V value = valueReaderProvider.get(copies, originalValueReader).read(entry, valueSize);

            return new WriteThroughEntry(key, value);
        }

        /**
         * Check there is no garbage in freeList.
         */
        private void checkConsistency() {
            readLock(null);
            try (SegmentState segmentState = SegmentState.get(SegmentState.getCopies(null))) {
                MultiStoreBytes entry = segmentState.tmpBytes;
                MultiMap hashLookup = hashLookup();
                for (long pos = 0L; (pos = freeList.nextSetBit(pos)) >= 0L; ) {
                    PosPresentOnce check = new PosPresentOnce(pos);
                    hashLookup.forEach(check);
                    if (check.count != 1)
                        throw new AssertionError();
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    long keySize = keySizeMarshaller.readSize(entry);
                    entry.skip(keySize);
                    manageReplicationBytes(segmentState, entry, false, false);
                    long valueSize = valueSizeMarshaller.readSize(entry);
                    long sizeInBytes = entrySize(keySize, valueSize);
                    int entrySizeInChunks = inChunks(sizeInBytes);
                    if (!freeList.allSet(pos, pos + entrySizeInChunks))
                        throw new AssertionError();
                    pos += entrySizeInChunks;
                }
            } finally {
                readUnlock();
            }
        }

        private class PosPresentOnce implements MultiMap.EntryConsumer {
            long pos;
            int count = 0;

            PosPresentOnce(long pos) {
                this.pos = pos;
            }

            @Override
            public void accept(long hash, long pos) {
                if (this.pos == pos) count++;
            }
        }
    }

    class EntryIterator implements Iterator<Entry<K, V>> {
        Entry<K, V> returnedEntry;
        // todo get rid of this
        MultiStoreBytes entry = new MultiStoreBytes();
        private int returnedSeg = -1;
        private long returnedPos = -1L;
        private int nextSeg;
        private long nextPos;

        EntryIterator() {
            advance(nextSeg = segments.length - 1, nextPos = -1L);
        }

        private boolean advance(int segIndex, long pos) {
            while (segIndex >= 0) {
                pos = segments[segIndex].hashLookup().getPositions().nextSetBit(pos + 1L);
                if (pos >= 0L) {
                    nextSeg = segIndex;
                    nextPos = pos;
                    return true;
                } else {
                    segIndex--;
                    pos = -1L;
                }
            }
            nextSeg = -1;
            nextPos = -1L;
            return false;
        }

        @Override
        public final boolean hasNext() {
            return nextSeg >= 0;
        }

        @Override
        public Entry<K, V> next() {
            for (; ; ) {
                int segIndex = nextSeg;
                long pos = nextPos;
                if (segIndex < 0)
                    throw new NoSuchElementException();
                final Segment segment = segments[segIndex];
                ThreadLocalCopies copies = SegmentState.getCopies(null);

                segment.readLock(null);
                try (SegmentState segmentState = SegmentState.get(copies)) {
                    if (segment.hashLookup().getPositions().isClear(pos)) {
                        // the pos was removed after the previous advance
                        advance(segIndex, pos);
                        continue;
                    }
                    advance(returnedSeg = segIndex, returnedPos = pos);
                    return returnedEntry = segment.getEntry(segmentState, pos);
                } finally {
                    segment.readUnlock();
                }
            }
        }

        @Override
        public final void remove() {
            int segIndex = returnedSeg;
            if (segIndex < 0)
                throw new IllegalStateException();
            VanillaChronicleMap.this.remove(returnedEntry.getKey());
            returnedSeg = -1;
            returnedEntry = null;
        }
    }

    class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @NotNull
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public final boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            try {
                V v = VanillaChronicleMap.this.get(e.getKey());
                return v != null && v.equals(e.getValue());
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
                return false;
            }
        }

        public final boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            try {
                Object key = e.getKey();
                Object value = e.getValue();
                return VanillaChronicleMap.this.remove(key, value);
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
                return false;
            }
        }

        public final int size() {
            return VanillaChronicleMap.this.size();
        }

        public final boolean isEmpty() {
            return VanillaChronicleMap.this.isEmpty();
        }

        public final void clear() {
            VanillaChronicleMap.this.clear();
        }
    }

    class WriteThroughEntry extends SimpleEntry<K, V> {
        private static final long serialVersionUID = 0L;

        WriteThroughEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public V setValue(V value) {
            put(getKey(), value);
            return super.setValue(value);
        }
    }

}
