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

import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.replication.*;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.lang.Maths;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.AllocateInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.NewInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import net.openhft.lang.values.LongValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.Objects.builderEquals;

/**
 * {@code AbstractChronicleMapBuilder} manages most of {@link ChronicleMap} configurations; has two concrete
 * subclasses: {@link OnHeapUpdatableChronicleMapBuilder} should be used to create maps with ordinary values,
 * {@link ChronicleMapBuilder} -- maps with {@link Byteable} values, which point directly to off-heap memory;
 * could be used as a classic builder and/or factory. This means
 * that in addition to the standard builder usage pattern: <pre>{@code
 * ChronicleMap<Key, Value> map = ChronicleMapOnHeapUpdatableBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500)
 *     // ... other configurations
 *     .create();}</pre>
 * one of concrete {@code AbstractChronicleMapBuilder} subclasses, {@link OnHeapUpdatableChronicleMapBuilder}
 * or {@link ChronicleMapBuilder}, could be prepared and used to create many similar
 * maps: <pre>{@code
 * ChronicleMapBuilder<Key, Value> builder = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500);
 *
 * ChronicleMap<Key, Value> map1 = builder.create();
 * ChronicleMap<Key, Value> map2 = builder.create();}</pre>
 * i. e. created {@code ChronicleMap} instances don't depend on the builder.
 *
 * <p>{@code AbstractChronicleMapBuilder} and it's subclasses are mutable, see a note in {@link
 * ChronicleHashBuilder} interface documentation. </p> <p>Later in this documentation, "ChronicleMap" means
 * "ChronicleMaps, created by {@code AbstractChronicleMapBuilder}", unless specified different, because
 * theoretically someone might provide {@code ChronicleMap} implementations with completely different
 * properties. </p> <p>{@code ChronicleMap} ("ChronicleMaps, created by {@code AbstractChronicleMapBuilder}")
 * currently doesn't support resizing. That is why you should <i>always</i> configure {@linkplain
 * #entries(long) number of entries} you are going to insert into the created map <i>at most</i>. See {@link
 * #entries(long)} method documentation for more information on this. </p> <p>{@code ChronicleMap} allocates
 * memory by equally sized chunks. This size is called {@linkplain #entrySize(int) entry size}, you are
 * strongly recommended to configure it to achieve least memory consumption and best speed. See {@link
 * #entrySize(int)} method documentation for more information on this. </p>
 *
 * @param <K> key type of the maps, produced by this builder
 * @param <V> value type of the maps, produced by this builder
 * @see ChronicleMap
 * @see ChronicleSetBuilder
 */
abstract class AbstractChronicleMapBuilder<K, V,
        B extends AbstractChronicleMapBuilder<K, V, B>>
        implements Cloneable, ChronicleHashBuilder<K, ChronicleMap<K, V>, B>, ChronicleMapBuilderI<K, V> {

    static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;
    private static final Bytes EMPTY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;
    private static final int MAX_SEGMENTS = (1 << 30);
    private static final int MAX_SEGMENTS_TO_CHAISE_COMPACT_MULTI_MAPS = (1 << 20);
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractChronicleMapBuilder.class.getName());


    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;

    private String name;

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private long actualEntriesPerSegment = -1L;
    private int keySize = 0;
    private K sampleKey;
    private int valueSize = 0;
    private V sampleValue;
    private int entrySize = 0;
    private Alignment alignment = null;
    private long entries = 1 << 20; // 1 million by default
    private long lockTimeOut = 2000;
    private TimeUnit lockTimeOutUnit = TimeUnit.MILLISECONDS;
    private int metaDataBytes = 0;
    private ChronicleHashErrorListener errorListener = ChronicleHashErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;

    // replication
    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;
    private MapEventListener<K, V, ChronicleMap<K, V>> eventListener =
            MapEventListeners.nop();
    private V defaultValue = null;
    private DefaultValueProvider<K, V> defaultValueProvider = null;
    private PrepareValueBytes<K, V> prepareValueBytes = null;

    private SingleChronicleHashReplication singleHashReplication = null;
    private InetSocketAddress[] pushToAddresses;
    public boolean bootstapOnlyLocalEntries = false;

    AbstractChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<K>(keyClass, SerializationBuilder.Role.KEY);
        valueBuilder = new SerializationBuilder<V>(valueClass, SerializationBuilder.Role.VALUE);
        if (valueClass == LongValue.class)
            prepareValueBytesOnAcquire(new ZeroOutValueBytes<K, V>(valueSize()));
    }

    protected static boolean offHeapReference(Class valueClass) {
        return Byteable.class.isAssignableFrom(valueClass);
    }


    private static long roundUpMapHeaderSize(long headerSize) {
        long roundUp = (headerSize + 127L) & ~127L;
        if (roundUp - headerSize < 64)
            roundUp += 128;
        return roundUp;
    }

    private static void checkSegments(int segments) {
        if (segments <= 0 || segments > MAX_SEGMENTS)
            throw new IllegalArgumentException("segments should be positive, " +
                    segments + " given");
        if (segments > MAX_SEGMENTS)
            throw new IllegalArgumentException("Max segments is " + MAX_SEGMENTS + ", " +
                    segments + " given");
    }

    private static long divideUpper(long dividend, long divisor) {
        return ((dividend - 1L) / divisor) + 1L;
    }

    private static String pretty(int value) {
        return value > 0 ? value + "" : "not configured";
    }

    private static String pretty(Object obj) {
        return obj != null ? obj + "" : "not configured";
    }

    @Override
    public ChronicleMapBuilderI<K, V> pushTo(InetSocketAddress... addresses) {
        this.pushToAddresses = addresses;
        return this;
    }

    @Override
    public B clone() {
        try {
            @SuppressWarnings("unchecked")
            final B result = (B) super.clone();
            result.keyBuilder = keyBuilder.clone();
            result.valueBuilder = valueBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    abstract B self();

    /**
     * {@inheritDoc}
     *
     * <p>Example: if keys in your map(s) are English words in {@link String} form, keys size 10
     * (a bit more than average English word length) would be a good choice: <pre>{@code
     * ChronicleMap<String, LongValue> wordFrequencies = ChronicleMapBuilder
     *     .of(String.class, LongValue.class)
     *     .entries(50000)
     *     .keySize(10)
     *     // shouldn't specify valueSize(), because it is statically known
     *     .create();}</pre>
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded (and each
     * character takes 2 bytes on-heap), because default off-heap {@link String} encoding is UTF-8 in {@code
     * ChronicleMap}.)
     *
     * @see #constantKeySizeBySample(Object)
     * @see #entrySize(int)
     */
    @Override
    public B keySize(int keySize) {
        if (keySize <= 0)
            throw new IllegalArgumentException("Key size must be positive");
        this.keySize = keySize;
        return self();
    }

    /**
     * {@inheritDoc}
     *
     * <p>For example, if your keys are Git commit hashes:<pre>{@code
     * Map<byte[], String> gitCommitMessagesByHash =
     *     ChronicleMapBuilder.of(byte[].class, String.class)
     *     .constantKeySizeBySample(new byte[20])
     *     .create();}</pre>
     *
     * @see #keySize(int)
     */
    @Override
    public B constantKeySizeBySample(K sampleKey) {
        this.sampleKey = sampleKey;
        return self();
    }

    private int keySize() {
        return keyOrValueSize(keySize, keyBuilder);
    }

    public B valueSize(int valueSize) {
        if (valueSize <= 0)
            throw new IllegalArgumentException("Value size must be positive");
        this.valueSize = valueSize;
        return self();
    }

    public B constantValueSizeBySample(V sampleValue) {
        this.sampleValue = sampleValue;
        return self();
    }

    int valueSize() {
        return keyOrValueSize(valueSize, valueBuilder);
    }

    private int keyOrValueSize(int configuredSize, SerializationBuilder builder) {
        if (configuredSize > 0)
            return configuredSize;
        // this means size is statically known
        if (builder.sizeMarshaller().sizeEncodingSize(0L) == 0)
            return (int) builder.sizeMarshaller().readSize(EMPTY_BYTES);
        return DEFAULT_KEY_OR_VALUE_SIZE;
    }

    /**
     * {@inheritDoc}
     *
     * <p>In fully default case you can expect entry size to be about 256 bytes. But it is strongly
     * recommended always to configure {@linkplain #keySize(int) key size} and {@linkplain #valueSize(int)
     * value size}, if they couldn't be derived statically. </p> <p>If entry size is not configured explicitly
     * by calling this method, it is computed based on {@linkplain #metaDataBytes(int) meta data bytes}, plus
     * {@linkplain #keySize(int) key size}, plus {@linkplain #valueSize(int) value size}, plus a few bytes
     * required by implementations. </p>
     *
     * @see #entries(long)
     */
    @Override
    public B entrySize(int entrySize) {
        if (entrySize <= 0)
            throw new IllegalArgumentException("Entry Size must be positive");
        this.entrySize = entrySize;
        return self();
    }

    int entrySize(boolean replicated) {
        if (entrySize > 0)
            return entryAndValueAlignment().alignSize(entrySize);
        int size = metaDataBytes;
        int keySize = keySize();
        size += keyBuilder.sizeMarshaller().sizeEncodingSize(keySize);
        size += keySize;
        if (replicated)
            size += ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;
        int valueSize = valueSize();
        size += valueBuilder.sizeMarshaller().sizeEncodingSize(valueSize);
        size = entryAndValueAlignment().alignSize(size);
        size += valueSize;
        // Some cache line heuristics
        for (int i = 1; i <= 4; i++) {
            int bound = i * 64;
            // Not more than 5% oversize.
            // DEFAULT_KEY_OR_VALUE_SIZE and this heuristic are specially adjusted to produce
            // entry size 256 -- the default value prior to keySize and valueSize -- when key and
            // value sizes are both not static nor configured, in both vanilla and replicated modes.
            if (size < bound && (bound - size) <= bound / 20) {
                size = bound;
                break;
            }
        }
        return entryAndValueAlignment().alignSize(size);
    }

    public B entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return self();
    }

    Alignment entryAndValueAlignment() {
        if (alignment != null)
            return alignment;
        return offHeapReference(valueBuilder.eClass) ? Alignment.OF_4_BYTES : Alignment.NO_ALIGNMENT;
    }

    @Override
    public B entries(long entries) {
        if (entries <= 0L)
            throw new IllegalArgumentException("Entries should be positive, " + entries + " given");
        this.entries = entries;
        return self();
    }

    long entries() {
        return entries;
    }

    @Override
    public B actualEntriesPerSegment(long actualEntriesPerSegment) {
        if (actualEntriesPerSegment <= 0L)
            throw new IllegalArgumentException("entries per segment should be positive, " +
                    actualEntriesPerSegment + " given");
        if (tooManyEntriesPerSegment(actualEntriesPerSegment))
            throw new IllegalArgumentException("max entries per segment is " +
                    MultiMapFactory.MAX_CAPACITY + ", " + actualEntriesPerSegment + " given");
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return self();
    }

    private boolean tooManyEntriesPerSegment(long entriesPerSegment) {
        return entriesPerSegment > MultiMapFactory.MAX_CAPACITY;
    }

    long actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0L)
            return actualEntriesPerSegment;
        int actualSegments = actualSegments();
        long actualEntries = totalEntriesIfPoorDistribution(actualSegments);
        long actualEntriesPerSegment = divideUpper(actualEntries, actualSegments);
        if (tooManyEntriesPerSegment(actualEntriesPerSegment))
            throw new IllegalStateException("max entries per segment is " +
                    MultiMapFactory.MAX_CAPACITY + " configured entries() and " +
                    "actualSegments() so that there should be " + actualEntriesPerSegment +
                    " entries per segment");
        return actualEntriesPerSegment;
    }

    private long totalEntriesIfPoorDistribution(int segments) {
        if (segments == 1)
            return entries;
        double poorDistEntriesScale = Math.log(segments) * entries;
        return Math.min(entries * segments,
                (long) (entries + poorDistEntriesScale * 0.14 + 32)); // 12% was min for tests
    }

    @Override
    public B minSegments(int minSegments) {
        checkSegments(minSegments);
        this.minSegments = minSegments;
        return self();
    }

    int minSegments() {
        return Math.max(estimateSegments(), minSegments);
    }

    private int estimateSegments() {
        return (int) Math.min(Maths.nextPower2(entries / 32, 1), estimateSegementBasedOnSize());

    }

    private int estimateSegementBasedOnSize() {
        // based on entries with multimap of 100 bytes.
        long size = (long) (Math.log10(entrySize + 32) / 2 * entries);
        if (size < 10 * 1000)
            return 512;
        if (size < 100 * 1000)
            return 1024;
        if (size < 1000 * 1000)
            return 2 * 1024;
        if (size < 10 * 1000 * 1000)
            return 4 * 1024;
        if (size < 100 * 1000 * 1000)
            return 8 * 1024;
        return 16 * 1024;
    }

    @Override
    public B actualSegments(int actualSegments) {
        checkSegments(actualSegments);
        this.actualSegments = actualSegments;
        return self();
    }

    int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        long shortMMapSegments = trySegments(MultiMapFactory.I16_MAX_CAPACITY,
                MAX_SEGMENTS_TO_CHAISE_COMPACT_MULTI_MAPS);
        if (shortMMapSegments > 0L)
            return (int) shortMMapSegments;
        long intMMapSegments = trySegments(MultiMapFactory.MAX_CAPACITY, MAX_SEGMENTS);
        if (intMMapSegments > 0L)
            return (int) intMMapSegments;
        throw new IllegalStateException("Max segments is " + MAX_SEGMENTS + ", configured so much" +
                " entries() that builder automatically decided to use " +
                (-intMMapSegments) + " segments");
    }

    private long trySegments(long maxSegmentCapacity, int maxSegments) {
        long segments = divideUpper(totalEntriesIfPoorDistribution(minSegments()),
                maxSegmentCapacity);
        segments = Maths.nextPower2(Math.max(segments, minSegments()), 1L);
        return segments <= maxSegments ? segments : -segments;
    }


    int segmentHeaderSize() {
        int segments = actualSegments();
        // reduce false sharing unless we have a lot of segments.
        return segments <= 16 * 1024 ? 64 : 32;
    }

    MultiMapFactory multiMapFactory() {
        return MultiMapFactory.forCapacity(actualEntriesPerSegment());
    }

    public B lockTimeOut(long lockTimeOut, TimeUnit unit) {
        this.lockTimeOut = lockTimeOut;
        lockTimeOutUnit = unit;
        return self();
    }

    long lockTimeOut(TimeUnit unit) {
        return unit.convert(lockTimeOut, lockTimeOutUnit);
    }

    @Override
    public B errorListener(ChronicleHashErrorListener errorListener) {
        this.errorListener = errorListener;
        return self();
    }

    ChronicleHashErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Configures if the maps created by this builder should return {@code null} instead of previous mapped
     * values on {@link ChronicleMap#put(Object, Object) ChornicleMap.put(key, value)} calls.
     *
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality which is rarely
     * used but fairly cheap for {@link HashMap}. In the case, for an off heap collection, it has to create a
     * new object and deserialize the data from off-heap memory. It's expensive for something you probably
     * don't use. </p> <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract
     * and returns the previous mapped value on {@code put()} calls. </p>
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but instead return
     *                       {@code null}
     * @return an instance of the map builder back
     * @see #removeReturnsNull(boolean)
     */
    public B putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return self();
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Configures if the maps created by this builder should return {@code null} instead of the last mapped
     * value on {@link ChronicleMap#remove(Object) ChronicleMap.remove(key)} calls.
     *
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is rarely
     * used but fairly cheap for {@link HashMap}. In the case, for an off heap collection, it has to create a
     * new object and deserialize the data from off-heap memory. It's expensive for something you probably
     * don't use. </p>
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry but
     *                          instead return {@code null}
     * @return an instance of the map builder back
     * @see #putReturnsNull(boolean)
     */
    public B removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return self();
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Override
    public B metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return self();
    }

    int metaDataBytes() {
        return metaDataBytes;
    }

    @Override
    public String toString() {
        return "ChronicleMapBuilder{" +
                "name=" + name +
                ", actualSegments=" + pretty(actualSegments) +
                ", minSegments=" + pretty(minSegments) +
                ", actualEntriesPerSegment=" + pretty(actualEntriesPerSegment) +
                ", keySize=" + pretty(keySize) +
                ", sampleKeyForConstantSizeComputation=" + pretty(sampleKey) +
                ", valueSize=" + pretty(valueSize) +
                ", sampleValueForConstantSizeComputation=" + pretty(sampleValue) +
                ", entrySize=" + pretty(entrySize) +
                ", entryAndValueAlignment=" + entryAndValueAlignment() +
                ", entries=" + entries() +
                ", lockTimeOut=" + lockTimeOut + " " + lockTimeOutUnit +
                ", metaDataBytes=" + metaDataBytes() +
                ", errorListener=" + errorListener() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerFactory=" + pretty(bytesMarshallerFactory) +
                ", objectSerializer=" + pretty(objectSerializer) +
                ", keyBuilder=" + keyBuilder +
                ", valueBuilder=" + valueBuilder +
                ", eventListener=" + eventListener +
                ", defaultValue=" + defaultValue +
                ", defaultValueProvider=" + pretty(defaultValueProvider) +
                ", prepareValueBytes=" + pretty(prepareValueBytes) +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return builderEquals(this, o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public B timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return self();
    }

    TimeProvider timeProvider() {
        return timeProvider;
    }

    BytesMarshallerFactory bytesMarshallerFactory() {
        return bytesMarshallerFactory == null ?
                bytesMarshallerFactory = new VanillaBytesMarshallerFactory() :
                bytesMarshallerFactory;
    }

    @Override
    public B bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return self();
    }

    ObjectSerializer objectSerializer() {
        return objectSerializer == null ?
                objectSerializer = BytesMarshallableSerializer.create(
                        bytesMarshallerFactory(), JDKObjectSerializer.INSTANCE) :
                objectSerializer;
    }

    @Override
    public B objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return self();
    }


    /**
     * {@inheritDoc}
     *
     * <p>Example:
     */
    @Override
    public B keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller) {
        keyBuilder.marshaller(keyMarshaller);
        return self();
    }

    @Override
    public B keyMarshallers(@NotNull BytesWriter<K> keyWriter, @NotNull BytesReader<K> keyReader) {
        keyBuilder.writer(keyWriter);
        keyBuilder.reader(keyReader);
        return self();
    }

    @Override
    public B keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller) {
        keyBuilder.sizeMarshaller(keySizeMarshaller);
        return self();
    }

    @Override
    public B keyDeserializationFactory(@NotNull ObjectFactory<K> keyDeserializationFactory) {
        keyBuilder.factory(keyDeserializationFactory);
        return self();
    }

    @Override
    public B immutableKeys() {
        keyBuilder.instancesAreMutable(false);
        return self();
    }

    @Override
    public StatelessClientConfig<ChronicleMap<K, V>> statelessClient(
            InetSocketAddress remoteAddress) {
        return new StatelessMapConfig<>(this.clone(), remoteAddress);
    }

    /**
     * Configures factory which is used to create a new value instance, if value class is either {@link
     * Byteable}, {@link BytesMarshallable} or {@link Externalizable} subclass in maps, created by this
     * builder.
     *
     * <p>Default value deserialization factory is {@link NewInstanceObjectFactory}, which creates a new value
     * instance using {@link Class#newInstance()} default constructor. You could provide an {@link
     * AllocateInstanceObjectFactory}, which uses {@code Unsafe.allocateInstance(Class)} (you might want to do
     * this for better performance or if you don't want to initialize fields), or a factory which calls a
     * value class constructor with some arguments, or a factory which internally delegates to instance pool
     * or {@link ThreadLocal}, to reduce allocations. </p>
     *
     * @param valueDeserializationFactory the value factory used to produce instances to deserialize data in
     * @return this builder back
     */
    public B valueDeserializationFactory(@NotNull ObjectFactory<V> valueDeserializationFactory) {
        valueBuilder.factory(valueDeserializationFactory);
        return self();
    }

    public B eventListener(MapEventListener<K, V, ChronicleMap<K, V>> eventListener) {
        this.eventListener = eventListener;
        return self();
    }

    MapEventListener<K, V, ChronicleMap<K, V>> eventListener() {
        return eventListener;
    }

    /**
     * Specifies the value to be put for each key queried in {@link ChronicleMap#acquireUsing acquireUsing()}
     * method, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValueProvider(DefaultValueProvider)}
     * configuration to this {@code AbstractChronicleMapBuilder}. </p>
     *
     * @param defaultValue the default value to be put to the map for absent keys during {@code
     *                     acquireUsing()} calls
     * @return this builder object back
     */
    public B defaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        this.defaultValueProvider = null;
        if (defaultValue == null)
            defaultValueProvider = NullValueProvider.INSTANCE;
        this.prepareValueBytes = null;
        return self();
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#acquireUsing
     * acquireUsing()} calls, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} configuration to this {@code
     * AbstractChronicleMapBuilder}. </p>
     *
     * @param defaultValueProvider the strategy to obtain a default value by the absent key
     * @return this builder object back
     */
    public B defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        this.defaultValue = null;
        this.prepareValueBytes = null;
        return self();
    }

    public B prepareValueBytesOnAcquire(@NotNull PrepareValueBytes<K, V> prepareValueBytes) {
        this.prepareValueBytes = prepareValueBytes;
        this.defaultValue = null;
        this.defaultValueProvider = null;
        return self();
    }

    PrepareValueBytesAsWriter<K> prepareValueBytesAsWriter() {
        if (prepareValueBytes == null)
            return null;
        return new PrepareValueBytesAsWriter<>(prepareValueBytes, valueSize());
    }


    /**
     * Non-public because should be called only after {@link #preMapConstruction(boolean)}
     */
    DefaultValueProvider<K, V> defaultValueProvider() {
        if (defaultValueProvider != null)
            return defaultValueProvider;
        if (defaultValue == null)
            return null;
        Object originalValueWriter = valueBuilder.interop();
        Provider writerProvider = Provider.of(originalValueWriter.getClass());
        ThreadLocalCopies copies = writerProvider.getCopies(null);
        Object valueWriter = writerProvider.get(copies, originalValueWriter);
        MetaProvider metaWriterProvider = valueBuilder.metaInteropProvider();
        copies = metaWriterProvider.getCopies(copies);
        MetaBytesWriter metaValueWriter = metaWriterProvider.get(copies,
                valueBuilder.metaInterop(), valueWriter, defaultValue);
        return new ConstantValueProvider<K, V>(defaultValue, metaValueWriter, valueWriter);
    }

    @Override
    public B replication(SingleChronicleHashReplication replication) {
        this.singleHashReplication = replication;
        return self();
    }


    @Override
    public B replication(byte identifier) {
        return replication(SingleChronicleHashReplication.builder().createWithId(identifier));
    }


    @Override
    public B replication(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return replication(SingleChronicleHashReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> instance() {
        return new InstanceConfig<>(this.clone(), singleHashReplication, null, null);
    }

    @Override
    public ChronicleMap<K, V> createPersistedTo(File file) throws IOException {
        // clone() to make this builder instance thread-safe, because createWithFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithFile(file, singleHashReplication, null);
    }

    @Override
    public ChronicleMap<K, V> create() {
        // clone() to make this builder instance thread-safe, because createWithoutFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithoutFile(singleHashReplication, null);
    }

    ChronicleMap<K, V> create(InstanceConfig<K, V> ib) throws IOException {
        if (ib.file != null) {
            return createWithFile(ib.file, ib.singleHashReplication, ib.channel);
        } else {
            return createWithoutFile(ib.singleHashReplication, ib.channel);
        }
    }

    ChronicleMap<K, V> createWithFile(File file, SingleChronicleHashReplication singleHashReplication,
                                      ReplicationChannel channel) throws IOException {
        pushingToMapEventListener();
        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                try (FileInputStream fis = new FileInputStream(file);
                     ObjectInputStream ois = new ObjectInputStream(fis)) {
                    VanillaChronicleMap<K, ?, ?, V, ?, ?> map =
                            (VanillaChronicleMap<K, ?, ?, V, ?, ?>) ois.readObject();
                    map.headerSize = roundUpMapHeaderSize(fis.getChannel().position());
                    map.createMappedStoreAndSegments(file);
                    return establishReplication(map, singleHashReplication, channel);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            if (file.createNewFile() || file.length() == 0) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        // new file
        if (!file.exists())
            throw new FileNotFoundException("Unable to create " + file);

        VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap(singleHashReplication, channel);

        try (FileOutputStream fos = new FileOutputStream(file);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(map);
            oos.flush();
            map.headerSize = roundUpMapHeaderSize(fos.getChannel().position());
            map.createMappedStoreAndSegments(file);
        }

        return establishReplication(map, singleHashReplication, channel);
    }

    /**
     * @param identifier id
     * @return map
     */
    public ChronicleMap<K, V> createReplicated(byte identifier) throws IOException {
        pushingToMapEventListener();
        preMapConstruction(true);
        VanillaChronicleMap<K, ?, ?, V, ?, ?> map =
                new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                        V, Object, MetaBytesWriter<V, Object>>(this, identifier);
        BytesStore bytesStore = new DirectStore(JDKObjectSerializer.INSTANCE,
                map.sizeInBytes(), true);
        map.createMappedStoreAndSegments(bytesStore);
        return map;
    }

    ChronicleMap<K, V> createWithoutFile(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel) {
        pushingToMapEventListener();
        try {
            VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap(singleHashReplication, channel);
            BytesStore bytesStore = new DirectStore(JDKObjectSerializer.INSTANCE,
                    map.sizeInBytes(), true);
            map.createMappedStoreAndSegments(bytesStore);
            return establishReplication(map, singleHashReplication, channel);
        } catch (IOException e) {
            // file-less version should never trigger an IOException.
            throw new AssertionError(e);
        }
    }

    private void pushingToMapEventListener() {
        if (pushToAddresses == null || pushToAddresses.length == 0) {
            return;
        }
        try {
            Class<?> pmel = Class.forName("com.higherfrequencytrading.chronicle.engine.map.PushingMapEventListener");
            Constructor<?> constructor = pmel.getConstructor(ChronicleMap[].class);
            // create a stateless client for each address
            ChronicleMap[] statelessClients = new ChronicleMap[pushToAddresses.length];
            ChronicleMapBuilderI<K, V> cmb = clone();
            cmb.pushTo((InetSocketAddress[]) null);
            for (int i = 0; i < pushToAddresses.length; i++) {
                cmb.statelessClient(pushToAddresses[i]);
                statelessClients[i] = cmb.create();
            }
            eventListener = (MapEventListener<K, V, ChronicleMap<K, V>>) constructor.newInstance(statelessClients);
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(getClass().getName()).warn("Chronicle Enterprise not found in the class path");
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass().getName()).error("PushingMapEventListener failed to load", e);
        }
    }

    ChronicleMap<K, V> createStatelessMap(StatelessMapConfig<K, V> statelessBuilder)
            throws IOException {
        pushingToMapEventListener();
        preMapConstruction(false);
        return new StatelessChronicleMap<K, V>(
                statelessBuilder,
                this);
    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel) throws IOException {
        boolean replicated = singleHashReplication != null || channel != null;
        preMapConstruction(replicated);
        if (replicated) {
            byte identifier;
            if (singleHashReplication != null) {
                identifier = singleHashReplication.identifier();
            } else {
                identifier = channel.hub().identifier();
            }
            return new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this, identifier);
        } else {
            return new VanillaChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        }
    }

    void preMapConstruction(boolean replicated) {
        keyBuilder.objectSerializer(objectSerializer());
        valueBuilder.objectSerializer(objectSerializer());

        long maxSize = (long) entrySize(replicated) * figureBufferAllocationFactor();
        keyBuilder.maxSize(maxSize);
        valueBuilder.maxSize(maxSize);

        if (sampleKey != null)
            keyBuilder.constantSizeBySample(sampleKey);
        if (sampleValue != null)
            valueBuilder.constantSizeBySample(sampleValue);
    }

    private ChronicleMap<K, V> establishReplication(
            VanillaChronicleMap<K, ?, ?, V, ?, ?> map, SingleChronicleHashReplication singleHashReplication,
            ReplicationChannel channel) throws IOException {
        if (map instanceof ReplicatedChronicleMap) {
            if (singleHashReplication != null && channel != null) {
                throw new AssertionError("Only one non-null replication should be passed");
            }
            ReplicatedChronicleMap result = (ReplicatedChronicleMap) map;
            List<Replicator> replicators = new ArrayList<>(2);
            if (singleHashReplication != null) {
                if (singleHashReplication.tcpTransportAndNetwork() != null)
                    replicators.add(Replicators.tcp(singleHashReplication));
                if (singleHashReplication.udpTransport() != null)
                    replicators.add(Replicators.udp(singleHashReplication.udpTransport()));
            } else {
                ReplicationHub hub = channel.hub();
                int entrySize = entrySize(true);
                if (entrySize > hub.maxEntrySize()) {
                    throw new IllegalArgumentException("During ChannelProviderBuilder setup, " +
                            "maxEntrySize=" + hub.maxEntrySize() +
                            " was specified, but map with " +
                            "entrySize=" + entrySize + " is attempted to apply to the replicator");
                }
                ChannelProvider provider = ChannelProvider.getProvider(hub);
                ChannelProvider.ChronicleChannel ch = provider.createChannel(channel.channelId());
                replicators.add(ch);
            }
            for (Replicator replicator : replicators) {
                Closeable token = replicator.applyTo(this, result, result, map);
                if (replicators.size() == 1 && token.getClass() == UdpReplicator.class) {
                    LOG.warn(Replicators.ONLY_UDP_WARN_MESSAGE);
                }
                result.addCloseable(token);
            }
        }
        return map;
    }

    private int figureBufferAllocationFactor() {
        // if expected map size is about 1000, seems rather wasteful to allocate
        // key and value serialization buffers each x64 of expected entry size..
        return (int) Math.min(Math.max(2L, entries() >> 10),
                VanillaChronicleMap.MAX_ENTRY_OVERSIZE_FACTOR);
    }

    @Override
    public ChronicleMapBuilderI<K, V> name(String name) {
        this.name = name;
        return this;
    }


    @Override
    public ChronicleMapBuilderI<K, V> bootstapOnlyLocalEntries(boolean bootstapOnlyLocalEntries) {
        this.bootstapOnlyLocalEntries = bootstapOnlyLocalEntries;
        return this;
    }

    public String name() {
        return this.name;
    }
}

