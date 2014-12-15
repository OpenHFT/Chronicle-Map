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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.hash.ChronicleHashErrorListener;
import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.*;
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

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.Class.forName;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.map.Asserts.assertNotNull;
import static net.openhft.lang.MemoryUnit.*;

class VanillaChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>> extends AbstractMap<K, V>
        implements ChronicleMap<K, V>, Serializable, ReadValue<V>, InstanceOrBytesToInstance,
        GetValueInterops<V, VI, MVI> {

//    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleMap.class);

    /**
     * Because DirectBitSet implementations couldn't find more than 64 continuous clear or set
     * bits.
     */
    static final int MAX_ENTRY_OVERSIZE_FACTOR = 64;
    private static final long serialVersionUID = 2L;
    final Class<K> kClass;
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
    //   private final int replicas;
    final long entrySize;
    final Alignment alignment;
    final int actualSegments;
    final long entriesPerSegment;
    final MapEventListener<K, V> eventListener;
    final BytesMapEventListener bytesEventListener;
    // if set the ReturnsNull fields will cause some functions to return NULL
    // rather than as returning the Object can be expensive for something you probably don't use.
    final boolean putReturnsNull;
    final boolean removeReturnsNull;
    private final long lockTimeOutNS;
    private final ChronicleHashErrorListener errorListener;
    private final int segmentHeaderSize;
    private final HashSplitting hashSplitting;
    final Class nativeValueClass;
    final MultiMapFactory multiMapFactory;
    final int maxEntryOversizeFactor;
    transient Provider<BytesReader<K>> keyReaderProvider;
    transient Provider<KI> keyInteropProvider;
    transient Provider<BytesReader<V>> valueReaderProvider;
    transient Provider<VI> valueInteropProvider;
    transient Segment[] segments; // non-final for close()
    // non-final for close() and because it is initialized out of constructor
    transient BytesStore ms;
    transient long headerSize;
    transient Set<Map.Entry<K, V>> entrySet;

    public VanillaChronicleMap(ChronicleMapBuilder<K, V> builder) throws IOException {

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
        prepareValueBytesAsWriter = builder.prepareValueBytesAsWriter();

        lockTimeOutNS = builder.lockTimeOut(TimeUnit.NANOSECONDS);

        boolean replicated = getClass() == ReplicatedChronicleMap.class;
        this.entrySize = builder.entrySize(replicated);
        this.alignment = builder.entryAndValueAlignment();

        this.errorListener = builder.errorListener();
        this.putReturnsNull = builder.putReturnsNull();
        this.removeReturnsNull = builder.removeReturnsNull();

        this.actualSegments = builder.actualSegments();
        this.entriesPerSegment = builder.actualEntriesPerSegment();
        this.multiMapFactory = builder.multiMapFactory();
        this.metaDataBytes = builder.metaDataBytes();
        this.eventListener = builder.eventListener();
        this.bytesEventListener = builder.bytesEventListener();
        this.segmentHeaderSize = builder.segmentHeaderSize();
        this.maxEntryOversizeFactor = builder.maxEntryOversizeFactor();

        hashSplitting = HashSplitting.Splitting.forSegments(actualSegments);
        initTransients();
    }

    final long segmentHash(long hash) {
        return hashSplitting.segmentHash(hash);
    }

    final int getSegment(long hash) {
        return hashSplitting.segmentIndex(hash);
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

        @SuppressWarnings("unchecked")
        Segment[] ss = (Segment[]) Array.newInstance(segmentType(), actualSegments);
        this.segments = ss;
    }

    Class segmentType() {
        return Segment.class;
    }

    final long createMappedStoreAndSegments(BytesStore bytesStore) throws IOException {
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
            NativeBytes segmentHeader =
                    (NativeBytes) segmentHeaders.bytes(headerOffset, segmentHeaderSize);
            NativeBytes segmentData = (NativeBytes) ms.bytes(offset, segmentSize);
            this.segments[i] = createSegment(segmentHeader, segmentData, i);
            headerOffset += segmentHeaderSize;
            offset += segmentSize;
        }
        return offset;
    }

    final long createMappedStoreAndSegments(File file) throws IOException {
        return createMappedStoreAndSegments(new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                sizeInBytes(), JDKObjectSerializer.INSTANCE));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
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

    Segment createSegment(NativeBytes segmentHeader, NativeBytes bytes, int index) {
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

    private long sizeOfMultiMapBitSet() {
        return MultiMapFactory.sizeOfBitSetInBytes(entriesPerSegment);
    }

    private long sizeOfBitSets() {
        return CACHE_LINES.align(BYTES.alignAndConvert(entriesPerSegment, BITS), BYTES);
    }

    private int numberOfBitSets() {
        return 1;
    }

    private long segmentSize() {
        long ss = (CACHE_LINES.align(sizeOfMultiMap() + sizeOfMultiMapBitSet(), BYTES)
                * multiMapsPerSegment())
                + numberOfBitSets() * sizeOfBitSets() // the free list and 0+ dirty lists.
                + sizeOfEntriesInSegment();
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

    private long sizeOfEntriesInSegment() {
        return CACHE_LINES.align(entriesPerSegment * entrySize, BYTES);
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

    final void put(Bytes entry, Bytes output) {
        put(entry, output, true);
    }

    final void putIfAbsent(Bytes entry, Bytes output) {
        put(entry, output, false);
    }

    private void put(Bytes entry, Bytes output, boolean replaceIfPresent) {
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
        private Bytes outputBytes;

        void reuse(SizeMarshaller valueSizeMarshaller, Bytes outputBytes) {
            valueSizeMarshaller(valueSizeMarshaller);
            this.outputBytes = outputBytes;
        }

        @Override
        Bytes bytes(long valueSize) {
            return outputBytes;
        }

        @Override
        public Bytes readValue(@NotNull ThreadLocalCopies copies, Bytes entry, Bytes usingBytes,
                               long valueSize) {
            outputBytes.writeBoolean(false);
            return super.readValue(copies, entry, usingBytes, valueSize);
        }

        @Override
        public Bytes readNull() {
            outputBytes.writeBoolean(true);
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

    @Override
    public final V put(K key, V value) {
        return put1(key, value, true);
    }

    @Override
    public final V putIfAbsent(@net.openhft.lang.model.constraints.NotNull K key, V value) {
        return put1(key, value, false);
    }

    V put1(@net.openhft.lang.model.constraints.NotNull K key, V value, boolean replaceIfPresent) {
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

    enum LockType {READ_LOCK, WRITE_LOCK}

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

    final void get(Bytes key, Bytes output) {
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
        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.alias("chronicle-entries", VanillaChronicleMap.EntrySet.class);
        // ideally we will use java serialization if we can
        lazyXStreamConverter().registerConverter(xstream);

        OutputStream outputStream = new FileOutputStream(toFile);
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            outputStream = new GZIPOutputStream(outputStream);
        try (OutputStream out = outputStream) {
            xstream.toXML(entrySet(), out);
        }
    }

    @Override
    public synchronized void putAll(File fromFile) throws IOException {
        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.alias("chronicle-entries", VanillaChronicleMap.EntrySet.class);

        // ideally we will use java serialization if we can
        lazyXStreamConverter().registerConverter(xstream);

        InputStream inputStream = new FileInputStream(fromFile);
        if (fromFile.getName().toLowerCase().endsWith(".gz"))
            inputStream = new GZIPInputStream(inputStream);
        try (InputStream out = inputStream) {
            xstream.fromXML(out);
        }
    }

    @Override
    public V newValueInstance() {
        if (vClass.equals(CharSequence.class) || vClass.equals(StringBuilder.class)) {
            return (V) new StringBuilder();
        }

        return newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return newInstance(kClass, true);
    }


    static <T> T newInstance(Class<T> aClass, boolean isKey) {
        try {
            return isKey ? DataValueClasses.newInstance(aClass) :
                    DataValueClasses.newDirectInstance(aClass);
        } catch (Exception e) {
            if (aClass.isInterface())
                throw new IllegalStateException("It not possible to create a instance from " +
                        "interface=" + aClass.getSimpleName() + " we recommend you create an " +
                        "instance in the usual way.");

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
                        " in the usual way, rather than using this method.");
            }
        }
    }

    private XStreamConverter xStreamConverter;

    /**
     * @return a lazily created XStreamConverter
     */
    private XStreamConverter lazyXStreamConverter() {
        return xStreamConverter != null ? xStreamConverter :
                (xStreamConverter = new XStreamConverter());
    }

    private class XStreamConverter {
        private Bytes buffer;

        public XStreamConverter() {
            this.buffer = new ByteBufferBytes(allocateDirect((int) entrySize)
                    .order(ByteOrder.nativeOrder()));
        }

        private void registerConverter(XStream xstream) {
            final Converter converter = new Converter() {
                final Bytes buffer = XStreamConverter.this.buffer;

                @Override
                public boolean canConvert(Class aClass) {
                    if (EntrySet.class.isAssignableFrom(aClass)
                            || WriteThroughEntry.class.isAssignableFrom(aClass))
                        return true;

                    if (Serializable.class
                            .isAssignableFrom(aClass))
                        return false;

                    if (vClass.equals(Map.class))
                        return false;
                    // todo add this back in when we do native
                    //  return kClass.isAssignableFrom(aClass) || vClass.isAssignableFrom(aClass);
                    return false;
                }

                @Override
                public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext
                        marshallingContext) {
                    if (WriteThroughEntry.class.isAssignableFrom(o.getClass())) {
                        final SimpleEntry e = (SimpleEntry) o;

                        writer.startNode("chronicle-key");

                        Object key = e.getKey();
                        if (!kClass.equals(key.getClass()))
                            writer.addAttribute("type", key.getClass().getCanonicalName());

                        marshallingContext.convertAnother(key);
                        writer.endNode();

                        writer.startNode("chronicle-value");
                        Object value = e.getValue();
                        if (!vClass.equals(value.getClass()))
                            writer.addAttribute("type", value.getClass().getCanonicalName());
                        marshallingContext.convertAnother(value);
                        writer.endNode();
                    } else if (EntrySet.class
                            .isAssignableFrom(o.getClass())) {
                        for (Entry e : (EntrySet) o) {
                            writer.startNode("chronicle-entry");
                            marshallingContext.convertAnother(e);
                            writer.endNode();
                        }
                    }
                }

                @Override
                public Object unmarshal(HierarchicalStreamReader reader,
                                        UnmarshallingContext unmarshallingContext) {
                    final String nodeName = reader.getNodeName();
                    switch (nodeName) {
                        case "chronicle-entries":

                            while (reader.hasMoreChildren()) {
                                reader.moveDown();
                                final String nodeName0 = reader.getNodeName();

                                if (!nodeName0.equals("chronicle-entry"))
                                    throw new ConversionException("unable to convert node " +
                                            "named=" + nodeName0);
                                K k = null;
                                V v = null;

                                while (reader.hasMoreChildren()) {
                                    reader.moveDown();
                                    final String nodeName1 = reader
                                            .getNodeName();
                                    if ("chronicle-key".equals(nodeName1))
                                        k = get(reader, unmarshallingContext, kClass);
                                    else if ("chronicle-value".equals(nodeName1))
                                        v = get(reader, unmarshallingContext, vClass);
                                    else
                                        throw new ConversionException("expecting either a key or value");
                                    reader.moveUp();
                                }

                                if (k != null)
                                    VanillaChronicleMap.this.put(k, v);

                                reader.moveUp();
                            }
                            break;
                        case "chronicle-key":
                            long keySize = keySizeMarshaller.readSize(buffer);
                            ThreadLocalCopies copies = keyReaderProvider.getCopies(null);
                            BytesReader<K> keyReader = keyReaderProvider.get(copies, originalKeyReader);
                            return keyReader.read(buffer, keySize);
                        case "chronicle-value":
                            long valueSize = valueSizeMarshaller.readSize(buffer);
                            copies = valueReaderProvider.getCopies(null);
                            BytesReader<V> valueReader =
                                    valueReaderProvider.get(copies, originalValueReader);
                            return valueReader.read(buffer, valueSize);
                    }

                    return null;
                }

                private <E> E get(HierarchicalStreamReader reader,
                                  UnmarshallingContext unmarshallingContext,
                                  Class<E> clazz) {
                    if (reader.getAttributeCount() > 0) {
                        final String type = reader.getAttribute("type");
                        try {
                            return (E) unmarshallingContext.convertAnother(null, forName(type));
                        } catch (ClassNotFoundException e) {
                            throw new ConversionException(e);
                        }
                    } else return (E) unmarshallingContext.convertAnother(null, clazz);
                }
            };

            xstream.registerConverter(converter);
        }
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

        lock.copies = copies;
        lock.metaKeyInterop = metaKeyInterop;
        lock.keyInterop = keyInterop;
        lock.keySize = keySize;
        lock.segmentHash = segmentHash;

        if (!nativeValueClass && lockType == LockType.WRITE_LOCK && v == null)
            v = usingValue;

        lock.value(v);
        lock.key(key);

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

    transient InstanceOrBytesToInstance<Bytes, K> keyBytesToInstance =
            new InstanceOrBytesToInstance<Bytes, K>() {
                @Override
                public K toInstance(@NotNull ThreadLocalCopies copies, Bytes keyBytes, long size) {
                    assertNotNull(copies);
                    BytesReader<K> keyReader = keyReaderProvider.get(copies, originalKeyReader);
                    return keyReader.read(keyBytes, size);
                }
            };

    transient InstanceOrBytesToInstance<Bytes, V> valueBytesToInstance =
            new InstanceOrBytesToInstance<Bytes, V>() {
                @Override
                public V toInstance(@NotNull ThreadLocalCopies copies, Bytes valueBytes, long size) {
                    return readValue(copies, valueBytes, null, size);
                }
            };

    transient InstanceOrBytesToInstance<Bytes, V> outputValueBytesToInstance =
            new InstanceOrBytesToInstance<Bytes, V>() {
                @Override
                public V toInstance(@NotNull ThreadLocalCopies copies, Bytes outputBytes, long size) {
                    outputBytes.position(outputBytes.position() - size);
                    return readValue(copies, outputBytes, null, size);
                }
            };

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

    final boolean containsKey(Bytes key) {
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
        return (entrySet != null) ? entrySet : (entrySet = newEntrySet());
    }

    Set<Entry<K, V>> newEntrySet() {
        return new EntrySet();
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
    public final boolean remove(@net.openhft.lang.model.constraints.NotNull final Object key, final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return (Boolean) removeIfValueIs(key, (V) value);
    }

    @Override
    public final VI getValueInterop(@NotNull ThreadLocalCopies copies) {
        assertNotNull(copies);
        return valueInteropProvider.get(copies, originalValueInterop);
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
            bytes.write(value);
        }
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

    final void removeKeyAsBytes(Bytes key) {
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

    final void remove(Bytes key, Bytes output) {
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
    final boolean removeWithValue(Bytes entry) {
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

    final void replaceKV(Bytes keyAndNewValue, Bytes output) {
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

    static final class SegmentState implements StatefulCopyable<SegmentState>, AutoCloseable {
        private static final Provider<SegmentState> segmentStateProvider =
                Provider.of(SegmentState.class);
        private static final SegmentState originalSegmentState = new SegmentState(0);

        static
        @NotNull
        ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            return segmentStateProvider.getCopies(copies);
        }

        static SegmentState get(@NotNull ThreadLocalCopies copies) {
            assertNotNull(copies);
            return segmentStateProvider.get(copies, originalSegmentState).get();
        }

        // inner state
        private final int depth;
        private boolean used;
        private SegmentState next;

        // segment state (reusable objects/fields)
        private final ReadLocked readLocked = new ReadLocked(this);
        private final NativeWriteLocked nativeWriteLocked = new NativeWriteLocked(this);
        private final HeapWriteLocked heapWriteLocked = new HeapWriteLocked(this);

        final MultiStoreBytes tmpBytes = new MultiStoreBytes();
        final GetRemoteBytesValueInterops getRemoteBytesValueInterops =
                new GetRemoteBytesValueInterops();
        final GetRemoteBytesValueInterops getRemoteBytesValueInterops2 =
                new GetRemoteBytesValueInterops();
        final ReadValueToOutputBytes readValueToOutputBytes = new ReadValueToOutputBytes();
        final ReadValueToBytes readValueToLazyBytes = new ReadValueToLazyBytes();

        long pos;
        long valueSizePos;

        private SegmentState(int depth) {
            if (depth > (1 << 16))
                throw new IllegalStateException("More than " + (1 << 16) +
                        " nested ChronicleMap contexts are not supported. Very probable that you " +
                        "simply forgot to close context somewhere (recommended to use " +
                        "try-with-resources statement). " +
                        "Otherwise this is a ChronicleMap bug, please report with this " +
                        "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues");
            this.depth = depth;
        }

        private SegmentState get() {
            if (!used) {
                used = true;
                return this;
            }
            if (next != null)
                return next.get();
            return next = new SegmentState(depth + 1);
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
            readLocked.segment = segment;
            readLocked.map = segment.map();
            return readLocked;
        }

        <K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
                V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        WriteLocked<K, KI, MKI, V, VI, MVI> nativeWriteLocked(VanillaChronicleMap.Segment segment) {
            nativeWriteLocked.segment = segment;
            nativeWriteLocked.map = segment.map();
            return nativeWriteLocked;
        }

        <K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
                V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        WriteLocked<K, KI, MKI, V, VI, MVI> heapWriteLocked(VanillaChronicleMap.Segment segment) {
            heapWriteLocked.segment = segment;
            heapWriteLocked.map = segment.map();
            return heapWriteLocked;
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
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final NativeBytes segmentHeader;
        final NativeBytes bytes;
        final long entriesOffset;
        private final int index;
        private final SingleThreadedDirectBitSet freeList;
        private MultiMap hashLookup;
        private long nextPosToSearchFrom = 0L;

        private VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map() {
            return VanillaChronicleMap.this;
        }

        /**
         * @param index the index of this segment held by the map
         */
        Segment(NativeBytes segmentHeader, NativeBytes bytes, int index) {
            this.segmentHeader = segmentHeader;
            this.bytes = bytes;
            this.index = index;

            long start = bytes.startAddr();
            hashLookup = createMultiMap(start);
            start += CACHE_LINES.align(sizeOfMultiMap() + sizeOfMultiMapBitSet(), BYTES)
                    * multiMapsPerSegment();
            final NativeBytes bsBytes = new NativeBytes(ms.objectSerializer(),
                    start,
                    start + LONGS.align(BYTES.alignAndConvert(entriesPerSegment, BITS), BYTES),
                    null);
            freeList = new SingleThreadedDirectBitSet(bsBytes);
            start += numberOfBitSets() * sizeOfBitSets();
            entriesOffset = start - bytes.startAddr();
            assert bytes.capacity() >= entriesOffset + entriesPerSegment * entrySize;
        }

        final MultiMap hashLookup() {
            return hashLookup;
        }

        private MultiMap createMultiMap(long start) {
            final NativeBytes multiMapBytes =
                    new NativeBytes((ObjectSerializer) null, start,
                            start = start + sizeOfMultiMap(), null);

            final NativeBytes sizeOfMultiMapBitSetBytes =
                    new NativeBytes((ObjectSerializer) null, start,
                            start + sizeOfMultiMapBitSet(), null);
//            multiMapBytes.load();
            return multiMapFactory.create(multiMapBytes, sizeOfMultiMapBitSetBytes);
        }

        public final int getIndex() {
            return index;
        }

        /* Methods with private access modifier considered private to Segment
         * class, although Java allows to access them from outer class anyway.
         */

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
                final boolean success = segmentHeader.tryRWWriteLock(LOCK_OFFSET, lockTimeOutNS);
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

        long startWriteLock = 0;

        final WriteLocked<K, KI, MKI, V, VI, MVI> writeLock() {
            return writeLock(null, false);
        }

        final WriteLocked<K, KI, MKI, V, VI, MVI> writeLock(
                SegmentState segmentState, boolean nativeValueClass) {
            startWriteLock = System.nanoTime();
            while (true) {
                final boolean success = segmentHeader.tryRWWriteLock(LOCK_OFFSET, lockTimeOutNS);
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
            if (lockTime > 1e8)
                System.out.printf("Thread took %,d ms to release lock%n", lockTime / 1000000);
        }

        @Override
        public final long offsetFromPos(long pos) {
            return entriesOffset + pos * entrySize;
        }

        final MultiStoreBytes reuse(MultiStoreBytes entry, long offset) {
            entry.setBytesOffset(bytes, offset);
            entry.position(metaDataBytes);
            return entry;
        }

        long entrySize(long keySize, long valueSize) {
            return alignment.alignAddr(metaDataBytes +
                    keySizeMarshaller.sizeEncodingSize(keySize) + keySize +
                    valueSizeMarshaller.sizeEncodingSize(valueSize)) + valueSize;
        }

        final int inBlocks(long sizeInBytes) {
            if (sizeInBytes <= entrySize)
                return 1;
            // int division is MUCH faster than long on Intel CPUs
            sizeInBytes -= 1L;
            if (sizeInBytes <= Integer.MAX_VALUE)
                return (((int) sizeInBytes) / (int) entrySize) + 1;
            return (int) (sizeInBytes / entrySize) + 1;
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV acquire(@Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                   MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                   InstanceOrBytesToInstance<KB, K> toKey,
                   ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                   long hash2, boolean create, MutableLockedEntry lock) {
            readLock(null);
            try {
                return acquireWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        readValue, usingValue, toValue, hash2, create, lock);
            } finally {
                if (segmentState != null)
                    segmentState.close();
                readUnlock();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>, RV>
        RV acquireWithoutLock(
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                ReadValue<RV> readValue, RV usingValue, InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, boolean create, MutableLockedEntry lock) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            SegmentState localSegmentState = segmentState;
            try {
                MultiStoreBytes entry = null;
                MultiMap hashLookup = this.hashLookup;
                hashLookup.startSearch(hash2);
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (localSegmentState == null) {
                            copies = SegmentState.getCopies(copies);
                            localSegmentState = SegmentState.get(copies);
                        }
                        entry = localSegmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);
                    return readValueAndNotifyGet(copies, key, keySize, toKey,
                            readValue, usingValue, toValue, entry);
                }
                // key is not found
                if (!create)
                    return readValue.readNull();
                if (entry == null) {
                    if (localSegmentState == null) {
                        copies = SegmentState.getCopies(copies); // after this, copies is not null
                        localSegmentState = SegmentState.get(copies);
                    }
                    entry = localSegmentState.tmpBytes;
                }

                RV result = createEntryOnAcquire(copies, localSegmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        readValue, usingValue, toValue, entry);

                //  notify the context that the entry was created
                if (lock instanceof WriteLocked)
                    ((WriteLocked) lock).created(true);

                return result;
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
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
                bytesEventListener.onPut(entry, 0L, keyPos, segmentState.valueSizePos, true);
            }
            if (eventListener != null) {
                eventListener.onPut(toKey.toInstance(copies, key, keySize),
                        toValue.toInstance(copies, v, valueSize), null);
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
            if (eventListener != null) {
                eventListener.onGetFound(toKey.toInstance(copies, key, keySize),
                        toValue.toInstance(copies, v, valueSize));
            }

            return v;
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV put3(@Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                long hash2, boolean replaceIfPresent,
                ReadValue<RV> readValue, boolean resultUnused) {
            shouldNotBeCalledFromReplicatedChronicleMap("Segment.put");
            writeLock();
            try {
                return putWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, value, toValue,
                        hash2, replaceIfPresent, readValue, resultUnused);
            } finally {
                if (segmentState != null)
                    segmentState.close();
                writeUnlock();
            }
        }

        <KB, KBI, MKBI extends MetaBytesInterop<KB, ? super KBI>,
                RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV putWithoutLock(
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                long hash2, boolean replaceIfPresent,
                ReadValue<RV> readValue, boolean resultUnused) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            SegmentState localSegmentState = segmentState;
            if (localSegmentState == null) {
                copies = SegmentState.getCopies(copies); // copies is not null now
                localSegmentState = SegmentState.get(copies);
            }
            try {
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = localSegmentState.tmpBytes;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);
                    if (replaceIfPresent) {
                        return replaceValueAndNotifyPut(copies, localSegmentState,
                                key, keySize, toKey,
                                getValueInterops, value, toValue,
                                entry, pos, offset, hashLookup, readValue, resultUnused,
                                false, false);
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
                long valueSize = metaValueInterop.size(valueInterop, value);
                putEntry(localSegmentState, metaKeyInterop, keyInterop, key, keySize,
                        metaValueInterop, valueInterop, value, entry, false);

                // put callbacks
                onPut(this, localSegmentState.pos);
                if (bytesEventListener != null)
                    bytesEventListener.onPut(entry, 0L, metaDataBytes,
                            localSegmentState.valueSizePos, true);
                if (eventListener != null)
                    eventListener.onPut(toKey.toInstance(copies, key, keySize),
                            toValue.toInstance(copies, value, valueSize), null);

                return resultUnused ? null : readValue.readNull();
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
        }

        final <KB, RV, VB extends RV, VBI, MVBI extends MetaBytesInterop<RV, ? super VBI>>
        RV replaceValueAndNotifyPut(
                ThreadLocalCopies copies, SegmentState segmentState,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB value,
                InstanceOrBytesToInstance<? super VB, V> toValue,
                MultiStoreBytes entry, long pos, long offset, MultiMap searchedHashLookup,
                ReadValue<RV> readValue, boolean resultUnused,
                boolean entryIsDeleted, boolean remote) {
            VBI valueInterop = getValueInterops.getValueInterop(copies);
            MVBI metaValueInterop = getValueInterops.getMetaValueInterop(
                    copies, valueInterop, value);
            long valueSize = metaValueInterop.size(valueInterop, value);

            long valueSizePos = entry.position();
            long prevValueSize = readValueSize(entry);
            long valueAddr = entry.positionAddr();
            long entryEndAddr = valueAddr + prevValueSize;

            RV prevValue = null;
            V prevValueInstance = null;
            if (!resultUnused && !entryIsDeleted)
                prevValue = readValue.readValue(copies, entry, null, prevValueSize);
            // todo optimize -- prevValue could be read twice
            if (eventListener != null && !entryIsDeleted) {
                entry.positionAddr(valueAddr);
                prevValueInstance = readValue(copies, entry, null, prevValueSize);
            }

            // putValue may relocate entry and change offset
            putValue(pos, offset, entry, valueSizePos, entryEndAddr, segmentState,
                    metaValueInterop, valueInterop, value, valueSize, searchedHashLookup);

            // put callbacks
            onPutMaybeRemote(segmentState.pos, remote);
            if (bytesEventListener != null)
                bytesEventListener.onPut(entry, 0L, metaDataBytes, valueSizePos, false);
            if (eventListener != null) {
                eventListener.onPut(toKey.toInstance(copies, key, keySize),
                        toValue.toInstance(copies, value, valueSize), prevValueInstance);
            }

            return resultUnused ? null : prevValue;
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
            long pos = alloc(inBlocks(entrySize));
            segmentState.pos = pos;
            long offset = offsetFromPos(pos);
            clearMetaData(offset);
            reuse(entry, offset);

            keySizeMarshaller.writeSize(entry, keySize);
            metaKeyInterop.write(keyInterop, entry, key);

            manageReplicationBytes(entry, writeDefaultInitialReplicationValues, false);

            segmentState.valueSizePos = entry.position();
            valueSizeMarshaller.writeSize(entry, valueSize);
            alignment.alignPositionAddr(entry);
            metaElemWriter.write(elemWriter, entry, elem);

            hashLookup.putAfterFailedSearch(pos);
            incrementSize();

            return valueSize;
        }

        void manageReplicationBytes(Bytes entry, boolean writeDefaultInitialReplicationValues,
                                    boolean remove) {
            // do nothing
        }

        private void clearMetaData(long offset) {
            if (metaDataBytes > 0)
                bytes.zeroOut(offset, offset + metaDataBytes);
        }

        //TODO refactor/optimize
        private long alloc(int blocks) {
            if (blocks > maxEntryOversizeFactor)
                throw new IllegalArgumentException("Entry is too large: requires " + blocks +
                        " entry size chucks, " + maxEntryOversizeFactor + " is maximum.");
            long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom, blocks);
            if (ret == DirectBitSet.NOT_FOUND || ret + blocks > entriesPerSegment) {
                if (ret + blocks > entriesPerSegment)
                    freeList.clear(ret, ret + blocks);
                ret = freeList.setNextNContinuousClearBits(0L, blocks);
                if (ret == DirectBitSet.NOT_FOUND || ret + blocks > entriesPerSegment) {
                    if (ret + blocks > entriesPerSegment)
                        freeList.clear(ret, ret + blocks);
                    if (blocks == 1) {
                        throw new IllegalStateException(
                                "Segment is full, no free entries found");
                    } else {
                        throw new IllegalStateException(
                                "Segment is full or has no ranges of " + blocks
                                        + " continuous free blocks"
                        );
                    }
                }
                updateNextPosToSearchFrom(ret, blocks);
            } else {
                // if bit at nextPosToSearchFrom is clear, it was skipped because
                // more than 1 block was requested. Don't move nextPosToSearchFrom
                // in this case. blocks == 1 clause is just a fast path.
                if (blocks == 1 || freeList.isSet(nextPosToSearchFrom)) {
                    updateNextPosToSearchFrom(ret, blocks);
                }
            }
            return ret;
        }

        private void updateNextPosToSearchFrom(long allocated, int blocks) {
            if ((nextPosToSearchFrom = allocated + blocks) >= entriesPerSegment)
                nextPosToSearchFrom = 0L;
        }

        private boolean realloc(long fromPos, int oldBlocks, int newBlocks) {
            if (freeList.allClear(fromPos + oldBlocks, fromPos + newBlocks)) {
                freeList.set(fromPos + oldBlocks, fromPos + newBlocks);
                return true;
            } else {
                return false;
            }
        }

        private void free(long fromPos, int blocks) {
            freeList.clear(fromPos, fromPos + blocks);
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
            writeLock();
            try {
                return removeWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key, keySize, toKey,
                        getValueInterops, expectedValue, toValue,
                        hash2, readValue, resultUnused);
            } finally {
                if (segmentState != null)
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
                @Nullable ThreadLocalCopies copies, final @Nullable SegmentState segmentState,
                MKBI metaKeyInterop, KBI keyInterop, KB key, long keySize,
                InstanceOrBytesToInstance<KB, K> toKey,
                GetValueInterops<VB, VBI, MVBI> getValueInterops, VB expectedValue,
                InstanceOrBytesToInstance<RV, V> toValue,
                long hash2, ReadValue<RV> readValue, boolean resultUnused) {
            segmentStateNotNullImpliesCopiesNotNull(copies, segmentState);
            SegmentState localSegmentState = segmentState;
            try {
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = null;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (localSegmentState == null) {
                            copies = SegmentState.getCopies(copies); // copies is not null now
                            localSegmentState = SegmentState.get(copies);
                        }
                        entry = localSegmentState.tmpBytes;
                    }
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
                    return removeEntry(copies, key, keySize, toKey, toValue,
                            readValue, resultUnused, hashLookup, entry, pos, valueSizePos,
                            valueSize, false, true, expectedValue != null);
                }
                // key is not found
                if (expectedValue == null) {
                    return resultUnused ? null : readValue.readNull();
                } else {
                    return Boolean.FALSE;
                }
            } finally {
                if (segmentState == null && localSegmentState != null)
                    localSegmentState.close();
            }
        }

        final <KB, RV, VB extends RV> Object removeEntry(
                ThreadLocalCopies copies,
                KB key, long keySize, InstanceOrBytesToInstance<KB, K> toKey,
                InstanceOrBytesToInstance<RV, V> toValue,
                ReadValue<RV> readValue, boolean resultUnused,
                MultiMap hashLookup, MultiStoreBytes entry, long pos,
                long valueSizePos, long valueSize, boolean remote, boolean removeFromMultiMap,
                boolean booleanResult) {
            // get the removed value, if needed
            RV removedValue = null;
            if ((!booleanResult && !resultUnused) || eventListener != null) {
                // todo reuse some value
                removedValue = readValue.readValue(copies, entry, null, valueSize);
                entry.position(entry.position() - valueSize);
            }

            // update segment state
            if (removeFromMultiMap) {
                hashLookup.removePrevPos();
                long entrySizeInBytes = entry.positionAddr() + valueSize - entry.startAddr();
                free(pos, inBlocks(entrySizeInBytes));
            } else {
                hashLookup.removePosition(pos);
            }
            decrementSize();

            // remove callbacks
            onRemoveMaybeRemote(pos, remote);
            if (bytesEventListener != null)
                bytesEventListener.onRemove(entry, 0L, metaDataBytes, valueSizePos);
            if (eventListener != null) {
                V removedValueForEventListener =
                        toValue.toInstance(copies, removedValue, valueSize);
                eventListener.onRemove(toKey.toInstance(copies, key, keySize),
                        removedValueForEventListener);
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
            SegmentState segmentState = null;
            try {
                MultiStoreBytes entry = null;
                MultiMap hashLookup = hashLookup();
                hashLookup.startSearch(hash2);
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        copies = SegmentState.getCopies(copies);
                        segmentState = SegmentState.get(copies);
                        entry = segmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    if (isDeleted(entry, keySize))
                        continue;
                    return true;
                }
                return false;
            } finally {
                if (segmentState != null)
                    segmentState.close();
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
            writeLock();
            try {
                hashLookup.startSearch(hash2);
                MultiStoreBytes entry = null;
                for (long pos; (pos = hashLookup.nextPos()) >= 0L; ) {
                    long offset = offsetFromPos(pos);
                    if (entry == null) {
                        if (segmentState == null) {
                            copies = SegmentState.getCopies(copies);
                            segmentState = SegmentState.get(copies);
                        }
                        entry = segmentState.tmpBytes;
                    }
                    reuse(entry, offset);
                    if (!keyEquals(keyInterop, metaKeyInterop, key, keySize, entry))
                        continue;
                    // key is found
                    entry.skip(keySize);

                    return onKeyPresentOnReplace(copies, segmentState, key, keySize, toKey,
                            getExpectedValueInterops, expectedValue, getNewValueInterops, newValue,
                            readValue, toValue, pos, offset, entry, hashLookup);
                }
                // key is not found
                return expectedValue == null ? null : Boolean.FALSE;
            } finally {
                if (segmentState != null)
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
                long pos, long offset, MultiStoreBytes entry, MultiMap searchedHashLookup) {
            long valueSizePos = entry.position();
            long valueSize = readValueSize(entry);
            long entryEndAddr = entry.positionAddr() + valueSize;
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
            putValue(pos, offset, entry, valueSizePos, entryEndAddr, segmentState,
                    metaValueInterop, valueInterop, newValue, newValueSize, searchedHashLookup);

            // put callbacks
            onPut(this, segmentState.pos);
            if (bytesEventListener != null) {
                long keyPos = metaDataBytes;
                bytesEventListener.onPut(entry, 0L, keyPos, segmentState.valueSizePos, true);
            }
            if (eventListener != null)
                eventListener.onPut(toKey.toInstance(copies, key, keySize),
                        toValue.toInstance(copies, newValue, newValueSize),
                        toValue.toInstance(copies, prevValue, valueSize));

            return expectedValue == null ? prevValue : Boolean.TRUE;
        }

        /**
         * Replaces value in existing entry. May cause entry relocation, because there may be not
         * enough space for new value in location already allocated for this entry.
         *
         * @param pos          index of the first block occupied by the entry
         * @param offset       relative offset of the entry in Segment bytes (before, i. e.
         *                     including metaData)
         * @param entry        relative pointer in Segment bytes
         * @param valueSizePos relative position of value size in entry
         * @param entryEndAddr absolute address of the entry end
         * @return relative offset of the entry in Segment bytes after putting value (that may cause
         * entry relocation)
         */
        final <E, EW> long putValue(
                long pos, long offset, MultiStoreBytes entry, long valueSizePos, long entryEndAddr,
                SegmentState segmentState,
                MetaBytesWriter<E, ? super EW> metaElemWriter, EW elemWriter, E newElem,
                long newElemSize,
                MultiMap searchedHashLookup) {
            long entryStartAddr = entry.address();
            long valueSizeAddr = entryStartAddr + valueSizePos;
            long newValueAddr = alignment.alignAddr(
                    valueSizeAddr + valueSizeMarshaller.sizeEncodingSize(newElemSize));
            long newEntryEndAddr = newValueAddr + newElemSize;
            newValueDoesNotFit:
            if (newEntryEndAddr != entryEndAddr) {
                long oldEntrySize = entryEndAddr - entryStartAddr;
                int oldSizeInBlocks = inBlocks(oldEntrySize);
                int newSizeInBlocks = inBlocks(newEntryEndAddr - entryStartAddr);
                if (newSizeInBlocks > oldSizeInBlocks) {
                    if (newSizeInBlocks > maxEntryOversizeFactor) {
                        throw new IllegalArgumentException("Value too large: " +
                                "entry takes " + newSizeInBlocks + " blocks, " +
                                maxEntryOversizeFactor + " is maximum.");
                    }
                    if (realloc(pos, oldSizeInBlocks, newSizeInBlocks))
                        break newValueDoesNotFit;
                    // RELOCATION
                    free(pos, oldSizeInBlocks);
                    onRelocation(this, pos);
                    pos = alloc(newSizeInBlocks);
                    // putValue() is called from put() and replace()
                    // after successful search by key
                    searchedHashLookup.replacePrevPos(pos);
                    offset = offsetFromPos(pos);
                    // Moving metadata, key size and key.
                    // Don't want to fiddle with pseudo-buffers for this,
                    // since we already have all absolute addresses.
                    long newEntryStartAddr = entry.address();
                    NativeBytes.UNSAFE.copyMemory(entryStartAddr,
                            newEntryStartAddr, valueSizeAddr - entryStartAddr);
                    entry = reuse(entry, offset);
                    // END OF RELOCATION
                } else if (newSizeInBlocks < oldSizeInBlocks) {
                    // Freeing extra blocks
                    freeList.clear(pos + newSizeInBlocks, pos + oldSizeInBlocks);
                    // Do NOT reset nextPosToSearchFrom, because if value
                    // once was larger it could easily became oversized again,
                    // But if these blocks will be taken by that time,
                    // this entry will need to be relocated.
                }
            }
            // Common code for all cases
            entry.position(valueSizePos);
            valueSizeMarshaller.writeSize(entry, newElemSize);
            alignment.alignPositionAddr(entry);
            metaElemWriter.write(elemWriter, entry, newElem);
            segmentState.pos = pos;
            return offset;
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

        boolean isDeleted(long pos) {
            return false;
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
                    manageReplicationBytes(entry, false, false);
                    long valueSize = valueSizeMarshaller.readSize(entry);
                    long sizeInBytes = entrySize(keySize, valueSize);
                    int entrySizeInBlocks = inBlocks(sizeInBytes);
                    if (!freeList.allSet(pos, pos + entrySizeInBlocks))
                        throw new AssertionError();
                    pos += entrySizeInBlocks;
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

    void shouldNotBeCalledFromReplicatedChronicleMap(String method) {
        // do nothing
    }

    class EntryIterator implements Iterator<Entry<K, V>> {
        Entry<K, V> returnedEntry;
        private int returnedSeg = -1;
        private long returnedPos = -1L;
        private int nextSeg;
        private long nextPos;

        // todo get rid of this
        MultiStoreBytes entry = new MultiStoreBytes();

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
                SegmentState segmentState = SegmentState.get(copies);
                try {
                    segment.readLock(null);
                    if (segment.hashLookup().getPositions().isClear(pos)) {
                        // the pos was removed after the previous advance
                        advance(segIndex, pos);
                        continue;
                    }
                    advance(returnedSeg = segIndex, returnedPos = pos);
                    return returnedEntry = segment.getEntry(segmentState, pos);
                } finally {
                    segmentState.close();
                    segment.readUnlock();
                }
            }
        }

        @Override
        public final void remove() {
            int segIndex = returnedSeg;
            if (segIndex < 0)
                throw new IllegalStateException();
            final Segment segment = segments[segIndex];
            final long pos = returnedPos;
            try {
                segment.writeLock();
                if (segment.hashLookup().getPositions().isClear(pos)) {
                    // The case:
                    // 1. iterator.next() - thread 1
                    // 2. map.put() which cause relocation of the key, returned in above - thread 2
                    // OR map.remove() which remove this key - thread 2
                    // 3. iterator.remove() - thread 1
                    segment.writeUnlock(); // not re-entrant.
                    VanillaChronicleMap.this.remove(returnedEntry.getKey());
                    segment.writeLock();
                } else {
                    removePresent(segment, pos);
                }
                returnedSeg = -1;
                returnedEntry = null;
            } finally {
                segment.writeUnlock();
            }
        }

        void removePresent(Segment segment, long pos) {
            // TODO handle the case:
            // iterator.next() -- thread 1
            // map.put() which cause relocation of the key, returned above -- thread 2
            // map.put() which place a new key on the `pos` in current segment -- thread 3
            // iterator.remove() -- thread 1
            // The simple solution is to compare bytes in the map with the serialized bytes
            // of returnedEntry.getKey(), but it seems rather wasteful to workaround so rare
            // case.
            final long offset = segment.offsetFromPos(pos);
            final NativeBytes entry = segment.reuse(this.entry, offset);

            final long keySize = keySizeMarshaller.readSize(entry);
            long position = entry.position();
            final long segmentHash = segmentHash(Hasher.hash(entry, position, position + keySize));

            removePresent(segment, pos, entry, keySize, segmentHash, true);
        }

        final void removePresent(Segment segment, long pos, NativeBytes entry, long keySize,
                                 long segmentHash, boolean removeFromMultiMap) {
            entry.skip(keySize);
            segment.manageReplicationBytes(entry, true, true);
            long valueSizePos = entry.position();
            long valueSize = readValueSize(entry);
            final long entryEndAddr = entry.positionAddr() + valueSize;
            if (removeFromMultiMap) {
                segment.hashLookup().remove(segmentHash, pos);
                segment.free(pos, segment.inBlocks(entryEndAddr - entry.address()));
            } else {
                segment.hashLookup().removePosition(pos);
            }
            segment.decrementSize();

            // remove callbacks
            onRemove(segment, pos);
            if (bytesEventListener != null)
                bytesEventListener.onRemove(entry, 0L, metaDataBytes, valueSizePos);
            if (eventListener != null)
                eventListener.onRemove(returnedEntry.getKey(), returnedEntry.getValue());
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

    static abstract class MutableLockedEntry<
            K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>> implements Context<K, V> {
        final SegmentState segmentState;

        VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map;
        VanillaChronicleMap<K, KI, MKI, V, VI, MVI>.Segment segment;
        ThreadLocalCopies copies;
        MKI metaKeyInterop;
        KI keyInterop;
        long keySize;
        long segmentHash;

        private K key;
        private V value;

        MutableLockedEntry(SegmentState segmentState) {
            this.segmentState = segmentState;
        }

        public final K key() {
            return key;
        }

        final void key(K key) {
            this.key = key;
        }

        public final V value() {
            return value;
        }

        void value(V value) {
            this.value = value;
        }

    }

    private static class ReadLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends MutableLockedEntry<K, KI, MKI, V, VI, MVI>
            implements ReadContext<K, V> {


        ReadLocked(SegmentState segmentState) {
            super(segmentState);
        }

        @Override
        public void close() {
            segmentState.close();
            segment.readUnlock();
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

        private boolean created = false;

        WriteLocked(SegmentState segmentState) {
            super(segmentState);
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
            segmentState.close();
            segment.writeUnlock();
        }

        @Override
        public void dontPutOnClose() {
            throw new IllegalStateException(
                    "This method is not supported for native value classes");
        }

        @Override
        public void removeEntry() {
            segment.removeWithoutLock(copies, segmentState,
                    metaKeyInterop, keyInterop, key(), keySize, map.keyIdentity(),
                    map, null, map.valueIdentity(), segmentHash, map, true);
        }
    }

    private static class HeapWriteLocked<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends WriteLocked<K, KI, MKI, V, VI, MVI> {

        private boolean putOnClose = true;

        HeapWriteLocked(SegmentState segmentState) {
            super(segmentState);
        }

        @Override
        public void close() {
            if (putOnClose) {
                segment.putWithoutLock(copies, segmentState,
                        metaKeyInterop, keyInterop, key(), keySize, map.keyIdentity(),
                        map, value(), map.valueIdentity(), segmentHash, true, map, true);
            }
            putOnClose = true;
            segmentState.close();
            segment.writeUnlock();
        }


        @Override
        public void dontPutOnClose() {
            putOnClose = false;
        }

        @Override
        public void removeEntry() {
            putOnClose = false;
            segment.removeWithoutLock(copies, segmentState,
                    metaKeyInterop, keyInterop, key(), keySize, map.keyIdentity(),
                    map, null, map.valueIdentity(), segmentHash, map, true);
        }
    }
}
