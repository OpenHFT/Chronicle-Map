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
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.DeserializationFactoryConfigurableBytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizeMarshallers;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.AllocateInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.NewInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.*;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.map.Objects.builderEquals;
import static net.openhft.lang.model.DataValueGenerator.firstPrimitiveFieldType;

/**
 * {@code ChronicleMapBuilder} manages {@link ChronicleMap} configurations; could be used as
 * a classic builder and/or factory. This means that in addition to the standard builder usage
 * pattern: <pre>{@code
 * ChronicleMap<Key, Value> map = ChronicleMapOnHeapUpdatableBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500)
 *     // ... other configurations
 *     .create();}</pre>
 * it could be prepared and used to create many similar maps: <pre>{@code
 * ChronicleMapBuilder<Key, Value> builder = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500);
 *
 * ChronicleMap<Key, Value> map1 = builder.create();
 * ChronicleMap<Key, Value> map2 = builder.create();}</pre>
 * i. e. created {@code ChronicleMap} instances don't depend on the builder.
 *
 * <p>{@code ChronicleMapBuilder} is mutable, see a note in {@link ChronicleHashBuilder} interface
 * documentation.
 *
 * <p>Later in this documentation, "ChronicleMap" means "ChronicleMaps, created by
 * {@code ChronicleMapBuilder}", unless specified different, because theoretically someone might
 * provide {@code ChronicleMap} implementations with completely different properties.
 *
 * <p>{@code ChronicleMap} ("ChronicleMaps, created by {@code ChronicleMapBuilder}") currently
 * doesn't support resizing. That is why you should <i>always</i> configure {@linkplain
 * #entries(long) number of entries} you are going to insert into the created map <i>at most</i>.
 * See {@link #entries(long)} method documentation for more information on this.
 *
 * <p>{@code ChronicleMap} allocates memory by equally sized chunks. This size is called {@linkplain
 * #entrySize(int) entry size}, you are strongly recommended to configure it to achieve least memory
 * consumption and best speed. See {@link #entrySize(int)} method documentation for more information
 * on this.
 *
 * @param <K> key type of the maps, produced by this builder
 * @param <V> value type of the maps, produced by this builder
 * @see ChronicleMap
 * @see ChronicleSetBuilder
 */
public final class ChronicleMapBuilder<K, V> implements Cloneable,
        ChronicleHashBuilder<K, ChronicleMap<K, V>, ChronicleMapBuilder<K, V>> {

    static final byte UDP_REPLICATION_MODIFICATION_ITERATOR_ID = (byte)128;
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;
    private static final int MAX_SEGMENTS = (1 << 30);
    private static final int MAX_SEGMENTS_TO_CHAISE_COMPACT_MULTI_MAPS = (1 << 20);
    private static final Logger LOG =
            LoggerFactory.getLogger(ChronicleMapBuilder.class.getName());
    public static final long DEFAULT_STATELESS_CLIENT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

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
    private int maxEntryOversizeFactor = -1;
    private Alignment alignment = null;
    private long entries = 1 << 20; // 1 million by default
    private long lockTimeOut = 20000;
    private TimeUnit lockTimeOutUnit = TimeUnit.MILLISECONDS;
    private int metaDataBytes = 0;
    private ChronicleHashErrorListener errorListener = ChronicleHashErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;

    // replication
    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;
    private MapEventListener<K, V> eventListener = null;
    private BytesMapEventListener bytesEventListener = null;
    private V defaultValue = null;
    private DefaultValueProvider<K, V> defaultValueProvider = null;
    private PrepareValueBytes<K, V> prepareValueBytes = null;

    private SingleChronicleHashReplication singleHashReplication = null;
    private InetSocketAddress[] pushToAddresses;

    ChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<>(keyClass, SerializationBuilder.Role.KEY);
        valueBuilder = new SerializationBuilder<>(valueClass, SerializationBuilder.Role.VALUE);
    }

    /**
     * Returns a new {@code ChronicleMapBuilder} instance which is able to {@linkplain #create()
     * create} maps with the specified key and value classes.
     *
     * @param keyClass   class object used to infer key type and discover it's properties
     *                   via reflection
     * @param valueClass class object used to infer value type and discover it's properties
     *                   via reflection
     * @param <K>        key type of the maps, created by the returned builder
     * @param <V>        value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
        return new ChronicleMapBuilder<>(keyClass, valueClass);
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

    public ChronicleMapBuilder<K, V> pushTo(InetSocketAddress... addresses) {
        this.pushToAddresses = addresses;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> clone() {
        try {
            @SuppressWarnings("unchecked")
            ChronicleMapBuilder<K, V> result =
                    (ChronicleMapBuilder<K, V>) super.clone();
            result.keyBuilder = keyBuilder.clone();
            result.valueBuilder = valueBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

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
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded (and
     * each character takes 2 bytes on-heap), because default off-heap {@link String} encoding
     * is UTF-8 in {@code ChronicleMap}.)
     *
     * @throws IllegalStateException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @see #constantKeySizeBySample(Object)
     * @see #valueSize(int)
     * @see #entrySize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> keySize(int keySize) {
        checkSizeIsNotStaticallyKnown(keyBuilder);
        if (keySize <= 0)
            throw new IllegalArgumentException("Key size must be positive");
        this.keySize = keySize;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>For example, if your keys are Git commit hashes:<pre>{@code
     * Map<byte[], String> gitCommitMessagesByHash =
     *     ChronicleMapBuilder.of(byte[].class, String.class)
     *     .constantKeySizeBySample(new byte[20])
     *     .immutableKeys()
     *     .create();}</pre>
     *
     * @see #keySize(int)
     * @see #constantValueSizeBySample(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> constantKeySizeBySample(K sampleKey) {
        this.sampleKey = sampleKey;
        return this;
    }

    private int keySize() {
        return keyOrValueSize(keySize, keyBuilder);
    }

    /**
     * Configures the optimal number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. If value size is always the same, call {@link
     * #constantValueSizeBySample(Object)} method instead of this one.
     *
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size
     * is known statically, it is automatically accounted and shouldn't be specified by user.
     *
     * <p>If value size varies moderately, specify the size higher than average, but lower than the
     * maximum possible, to minimize average memory overuse. If value size varies in a wide range,
     * it's better to use {@linkplain #entrySize(int) entry size} in "chunk" mode and configure it
     * directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this builder back
     * @throws IllegalStateException if value size is known statically and shouldn't be configured
     *         by user
     * @throws IllegalArgumentException if the given {@code valueSize} is non-positive
     * @see #constantValueSizeBySample(Object)
     * @see #keySize(int)
     * @see #entrySize(int)
     */
    public ChronicleMapBuilder<K, V> valueSize(int valueSize) {
        checkSizeIsNotStaticallyKnown(valueBuilder);
        if (valueSize <= 0)
            throw new IllegalArgumentException("Value size must be positive");
        this.valueSize = valueSize;
        return this;
    }

    private static void checkSizeIsNotStaticallyKnown(SerializationBuilder builder) {
        if (builder.sizeIsStaticallyKnown)
            throw new IllegalStateException("Size of type " + builder.eClass +
                    " is statically known and shouldn't be specified manually");
    }

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. This is done by providing the {@code sampleValue}, all values should
     * take the same number of bytes in serialized form, as this sample object.
     *
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size
     * is known statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If value size varies, method {@link #valueSize(int)} or {@link #entrySize(int)} should be
     * called instead of this one.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #valueSize(int)
     * @see #constantKeySizeBySample(Object)
     */
    public ChronicleMapBuilder<K, V> constantValueSizeBySample(V sampleValue) {
        this.sampleValue = sampleValue;
        return this;
    }

    int valueSize() {
        return keyOrValueSize(valueSize, valueBuilder);
    }

    private int keyOrValueSize(int configuredSize, SerializationBuilder builder) {
        if (configuredSize > 0)
            return configuredSize;
        if (builder.constantSizeMarshaller())
            return builder.pseudoReadConstantSize();
        return DEFAULT_KEY_OR_VALUE_SIZE;
    }

    /**
     * {@inheritDoc}
     *
     * <p>In fully default case you can expect entry size to be about 256 bytes. But it is strongly
     * recommended always to configure {@linkplain #keySize(int) key size} and {@linkplain
     * #valueSize(int) value size}, if they couldn't be derived statically.
     *
     * <p>If entry size is not configured explicitly by calling this method, it is computed based on
     * {@linkplain #metaDataBytes(int) meta data bytes}, plus {@linkplain #keySize(int) key size},
     * plus {@linkplain #valueSize(int) value size}, plus a few bytes required by implementations,
     * with respect to {@linkplain #entryAndValueAlignment(Alignment) alignment}.
     *
     * <p>Note that the actual entrySize will be aligned by the configured {@linkplain
     * #entryAndValueAlignment(Alignment) entry and value alignment}. I. e. if you set entry size
     * to 30, and entry alignment is set to {@link Alignment#OF_4_BYTES}, the actual entry size will
     * be 32 (30 aligned to 4 bytes).
     *
     * @throws IllegalStateException is sizes of both keys and values of maps created by this
     *         builder are statically known, hence entry size shouldn't be configured by user
     * @see #entryAndValueAlignment(Alignment)
     * @see #entries(long)
     * @see #maxEntryOversizeFactor(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> entrySize(int entrySize) {
        if (keyBuilder.sizeIsStaticallyKnown && valueBuilder.sizeIsStaticallyKnown) {
            throw new IllegalStateException("Sizes of key type: " + keyBuilder.eClass + " and " +
                    "value type: " + valueBuilder.eClass + " are both statically known, " +
                    "so entry size shouldn't be specified manually");
        }
        if (entrySize <= 0)
            throw new IllegalArgumentException("Entry Size must be positive");
        this.entrySize = entrySize;
        return this;
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

    @Override
    public ChronicleMapBuilder<K, V> maxEntryOversizeFactor(int maxEntryOversizeFactor) {
        if (maxEntryOversizeFactor < 1 || maxEntryOversizeFactor > 64)
            throw new IllegalArgumentException("maxEntryOversizeFactor should be in [1, 64] " +
                    "range, " + maxEntryOversizeFactor + " given");
        this.maxEntryOversizeFactor = maxEntryOversizeFactor;
        return this;
    }

    int maxEntryOversizeFactor() {
        if (maxEntryOversizeFactor < 0)
            return keyBuilder.constantSizeMarshaller() && valueBuilder.constantSizeMarshaller() ?
                    1 : 64;
        return maxEntryOversizeFactor;
    }

    /**
     * Configures alignment strategy of address in memory of entries and independently of address in
     * memory of values within entries in ChronicleMaps, created by this builder.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with volatile
     * access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>Note that {@linkplain #entrySize(int) entry size} will be aligned according to this
     * alignment. I. e. if you set {@code entrySize(20)} and {@link Alignment#OF_8_BYTES}, actual
     * entry size will be 24 (20 aligned to 8 bytes).
     *
     * <p>If values couldn't reference off-heap memory (i. e. it is not {@link Byteable} or
     * "data value generated"), alignment configuration makes no sense and forbidden.
     *
     * <p>Default is {@link Alignment#NO_ALIGNMENT} if values couldn't reference off-heap memory,
     * otherwise chosen heuristically (configure explicitly for being sure and to compare
     * performance in your case).
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     * @throws IllegalStateException if values of maps, created by this builder, couldn't reference
     *         off-heap memory
     */
    public ChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        checkAlignmentOnlyIfValuesPossiblyReferenceOffHeap();
        return this;
    }

    private void checkAlignmentOnlyIfValuesPossiblyReferenceOffHeap() {
        if (!valueBuilder.possibleOffHeapReferences() &&
                (alignment == Alignment.OF_4_BYTES || alignment == Alignment.OF_8_BYTES))
            throw new IllegalStateException("Entry and value alignment should be configured only " +
                    "if values might point to off-heap memory");
    }

    Alignment entryAndValueAlignment() {
        if (alignment != null)
            return alignment;
        Class firstPrimitiveFieldType = firstPrimitiveFieldType(valueBuilder.eClass);
        if (firstPrimitiveFieldType == long.class || firstPrimitiveFieldType == double.class)
            return Alignment.OF_8_BYTES;
        if (firstPrimitiveFieldType == int.class || firstPrimitiveFieldType == float.class)
            return Alignment.OF_4_BYTES;
        return Alignment.NO_ALIGNMENT;
    }

    /**
     * Package-private for tests
     */


    @Override
    public ChronicleMapBuilder<K, V> entries(long entries) {
        if (entries <= 0L)
            throw new IllegalArgumentException("Entries should be positive, " + entries + " given");
        this.entries = entries;
        return this;
    }

    long entries() {
        return entries;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualEntriesPerSegment(long actualEntriesPerSegment) {
        if (actualEntriesPerSegment <= 0L)
            throw new IllegalArgumentException("entries per segment should be positive, " +
                    actualEntriesPerSegment + " given");
        if (tooManyEntriesPerSegment(actualEntriesPerSegment))
            throw new IllegalArgumentException("max entries per segment is " +
                    MultiMapFactory.MAX_CAPACITY + ", " + actualEntriesPerSegment + " given");
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
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
    public ChronicleMapBuilder<K, V> minSegments(int minSegments) {
        checkSegments(minSegments);
        this.minSegments = minSegments;
        return this;
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
    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        checkSegments(actualSegments);
        this.actualSegments = actualSegments;
        return this;
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

    public ChronicleMapBuilder<K, V> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        this.lockTimeOut = lockTimeOut;
        lockTimeOutUnit = unit;
        return this;
    }

    long lockTimeOut(TimeUnit unit) {
        return unit.convert(lockTimeOut, lockTimeOutUnit);
    }

    @Override
    public ChronicleMapBuilder<K, V> errorListener(ChronicleHashErrorListener errorListener) {
        this.errorListener = errorListener;
        return this;
    }

    ChronicleHashErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Configures if the maps created by this builder should return {@code null} instead of previous
     * mapped values on {@link ChronicleMap#put(Object, Object) ChornicleMap.put(key, value)} calls.
     *
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality which
     * is rarely used but fairly cheap for {@link HashMap}. In the case, for an off heap collection,
     * it has to create a new object and deserialize the data from off-heap memory. It's expensive
     * for something you probably don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract
     * and returns the previous mapped value on {@code put()} calls.
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but
     *                       instead return {@code null}
     * @return this builder back
     * @see #removeReturnsNull(boolean)
     */
    public ChronicleMapBuilder<K, V> putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Configures if the maps created by this builder should return {@code null} instead of the last
     * mapped value on {@link ChronicleMap#remove(Object) ChronicleMap.remove(key)} calls.
     *
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is
     * rarely used but fairly cheap for {@link HashMap}. In the case, for an off heap collection,
     * it has to create a new object and deserialize the data from off-heap memory. It's expensive
     * for something you probably don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract
     * and returns the mapped value on {@code remove()} calls.
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry
     *                          but instead return {@code null}
     * @return this builder back
     * @see #putReturnsNull(boolean)
     */
    public ChronicleMapBuilder<K, V> removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Override
    public ChronicleMapBuilder<K, V> metaDataBytes(int metaDataBytes) {
        if (metaDataBytes < 0 || metaDataBytes > 255)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return this;
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
    public ChronicleMapBuilder<K, V> timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
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
    public ChronicleMapBuilder<K, V> bytesMarshallerFactory(
            BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    ObjectSerializer acquireObjectSerializer(ObjectSerializer defaultSerializer) {
        return objectSerializer == null ?
                BytesMarshallableSerializer.create(bytesMarshallerFactory(), defaultSerializer) :
                objectSerializer;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Example: <pre>{@code Map<Key, Value> map =
     *     ChronicleMapBuilder.of(Key.class, Value.class)
     *     .entries(1_000_000)
     *     .keySize(50).valueSize(200)
     *     // this class hasn't implemented yet, just for example
     *     .objectSerializer(new KryoObjectSerializer())
     *     .create();}</pre>
     *
     * <p>This serializer is used to serialize both keys and values, if they both require this:
     * loosely typed, nullable, and custom {@linkplain #keyMarshaller(BytesMarshaller) key} and
     * {@linkplain #valueMarshaller(BytesMarshaller) value} marshallers are not configured.
     */
    @Override
    public ChronicleMapBuilder<K, V> objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshaller(
            @NotNull BytesMarshaller<? super K> keyMarshaller) {
        keyBuilder.marshaller(keyMarshaller);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshallers(
            @NotNull BytesWriter<K> keyWriter, @NotNull BytesReader<K> keyReader) {
        keyBuilder.writer(keyWriter);
        keyBuilder.reader(keyReader);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller) {
        keyBuilder.sizeMarshaller(keySizeMarshaller);
        return this;
    }

    /**
     * @throws IllegalStateException {@inheritDoc}
     * @see #valueDeserializationFactory(ObjectFactory)
     */
    @Override
    public ChronicleMapBuilder<K, V> keyDeserializationFactory(
            @NotNull ObjectFactory<K> keyDeserializationFactory) {
        keyBuilder.factory(keyDeserializationFactory);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> immutableKeys() {
        keyBuilder.instancesAreMutable(false);
        return this;
    }

    /**
     * Configures the {@code BytesMarshaller} used to serialize/deserialize values to/from off-heap
     * memory in maps, created by this builder. See <a
     * href="https://github.com/OpenHFT/Chronicle-Map#serialization">the section about serialization
     * in ChronicleMap manual</a> for more information.
     *
     * @param valueMarshaller the marshaller used to serialize values
     * @return this builder back
     * @see #valueMarshallers(BytesWriter, BytesReader)
     * @see #objectSerializer(ObjectSerializer)
     * @see #keyMarshaller(BytesMarshaller)
     */
    public ChronicleMapBuilder<K, V> valueMarshaller(
            @NotNull BytesMarshaller<? super V> valueMarshaller) {
        valueBuilder.marshaller(valueMarshaller);
        return this;
    }

    /**
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory
     * in maps, created by this builder. See <a
     * href="https://github.com/OpenHFT/Chronicle-Map#serialization">the section about serialization
     * in ChronicleMap manual</a> for more information.
     *
     * <p>Configuring marshalling this way results to a little bit more compact in-memory layout
     * of the map, comparing to a single interface configuration: {@link #valueMarshaller(
     * BytesMarshaller)}.
     *
     * <p>Passing {@link BytesInterop} instead of plain {@link BytesWriter} is, of cause, possible,
     * but currently pointless for values.
     *
     * @param valueWriter the new value object &rarr; {@link Bytes} writer (interop) strategy
     * @param valueReader the new {@link Bytes} &rarr; value object reader strategy
     * @return this builder back
     * @see #valueMarshaller(BytesMarshaller)
     * @see #valueSizeMarshaller(SizeMarshaller)
     * @see #keyMarshallers(BytesWriter, BytesReader)
     */
    public ChronicleMapBuilder<K, V> valueMarshallers(
            @NotNull BytesWriter<V> valueWriter, @NotNull BytesReader<V> valueReader) {
        valueBuilder.writer(valueWriter);
        valueBuilder.reader(valueReader);
        return this;
    }

    /**
     * Configures the marshaller used to serialize actual value sizes to off-heap memory in maps,
     * created by this builder.
     *
     * <p>Default value size marshaller is so-called {@linkplain SizeMarshallers#stopBit() stop bit
     * encoding marshalling}, unless {@link #constantValueSizeBySample(Object)} or the builder
     * statically knows the value size is constant -- {@link SizeMarshallers#constant(long)} or
     * equivalents are used by default in these cases.
     *
     * @param valueSizeMarshaller the new marshaller, used to serialize actual value sizes
     *                            to off-heap memory
     * @return this builder back
     * @see #keySizeMarshaller(SizeMarshaller)
     */
    public ChronicleMapBuilder<K, V> valueSizeMarshaller(
            @NotNull SizeMarshaller valueSizeMarshaller) {
        valueBuilder.sizeMarshaller(valueSizeMarshaller);
        return this;
    }

    /**
     * Configures factory which is used to create a new value instance, if value class is either
     * {@link Byteable}, {@link BytesMarshallable} or {@link Externalizable} subclass, or value type
     * is eligible for data value generation, or configured custom value reader is an instance of
     * {@link DeserializationFactoryConfigurableBytesReader}, in maps, created by this builder.
     *
     * <p>Default value deserialization factory is {@link NewInstanceObjectFactory}, which creates
     * a new value instance using {@link Class#newInstance()} default constructor. You could provide
     * an {@link AllocateInstanceObjectFactory}, which uses {@code Unsafe.allocateInstance(Class)}
     * (you might want to do this for better performance or if you don't want to initialize fields),
     * or a factory which calls a value class constructor with some arguments, or a factory which
     * internally delegates to instance pool or {@link ThreadLocal}, to reduce allocations.
     *
     * @param valueDeserializationFactory the value factory used to produce instances to deserialize
     *                                    data in
     * @return this builder back
     * @throws IllegalStateException if it is not possible to apply deserialization factory to
     *                               value deserializers, currently configured for this builder
     * @see #keyDeserializationFactory(ObjectFactory)
     */
    public ChronicleMapBuilder<K, V> valueDeserializationFactory(
            @NotNull ObjectFactory<V> valueDeserializationFactory) {
        valueBuilder.factory(valueDeserializationFactory);
        return this;
    }

    public ChronicleMapBuilder<K, V> eventListener(MapEventListener<K, V> eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    MapEventListener<K, V> eventListener() {
        return eventListener;
    }

    public ChronicleMapBuilder<K, V> bytesEventListener(BytesMapEventListener eventListener) {
        this.bytesEventListener = eventListener;
        return this;
    }

    BytesMapEventListener bytesEventListener() {
        return bytesEventListener;
    }

    /**
     * Specifies the value to be put for each key queried in {@link ChronicleMap#acquireUsing
     * acquireUsing()} method, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValueProvider(
     * DefaultValueProvider)} and {@link #prepareDefaultValueBytes(PrepareValueBytes)}
     * configurations to this builder.
     *
     * @param defaultValue the default value to be put to the map for absent keys during {@code
     *                     acquireUsing()} calls
     * @return this builder object back
     * @see #defaultValueProvider(DefaultValueProvider)
     * @see #prepareDefaultValueBytes(PrepareValueBytes)
     */
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        this.defaultValueProvider = null;
        if (defaultValue == null)
            defaultValueProvider = NullValueProvider.INSTANCE;
        this.prepareValueBytes = null;
        return this;
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#acquireUsing
     * acquireUsing()} calls, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} and {@link
     * #prepareDefaultValueBytes(PrepareValueBytes)} configurations to this builder.
     *
     * @param defaultValueProvider the strategy to obtain a default value by the absent key
     * @return this builder object back
     * @see #defaultValue(Object)
     * @see #prepareDefaultValueBytes(PrepareValueBytes)
     */
    public ChronicleMapBuilder<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        this.defaultValue = null;
        this.prepareValueBytes = null;
        return this;
    }

    /**
     * Configures the procedure which is called on the bytes, which later the returned value is
     * pointing to or deserialized from, if the key is absent, on {@link ChronicleMap#acquireUsing
     * acquireUsing()} call on maps, created by this builder. See {@link PrepareValueBytes} for more
     * information.
     *
     * <p>This method of value initialization on {@code acquireUsing()} calls is allowed only if
     * value size is constant. Otherwise you should use either {@link #defaultValue(Object)} or
     * {@link #defaultValueProvider(DefaultValueProvider)} methods.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} and {@link
     * #defaultValueProvider(DefaultValueProvider)} configurations to this builder.
     *
     * <p>The default preparation callback zeroes out the value bytes.
     *
     * @param prepareValueBytes what to do with the value bytes before assigning them into the
     *                          {@link Byteable} value or deserializing the value from,
     *                          to return from {@code acquireUsing()} call
     * @return this builder back
     * @throws IllegalStateException is value size is not constant
     * @see PrepareValueBytes
     * @see #defaultValue(Object)
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    public ChronicleMapBuilder<K, V> prepareDefaultValueBytes(
            @NotNull PrepareValueBytes<K, V> prepareValueBytes) {
        this.prepareValueBytes = prepareValueBytes;
        this.defaultValue = null;
        this.defaultValueProvider = null;
        checkPrepareValueBytesOnlyIfConstantValueSize();
        return this;
    }

    private void checkPrepareValueBytesOnlyIfConstantValueSize() {
        if (prepareValueBytes != null && !valueBuilder.constantSizeMarshaller())
            throw new IllegalStateException("Prepare value bytes could be used only if " +
                "value size is constant");
    }

    PrepareValueBytesAsWriter<K> prepareValueBytesAsWriter() {
        PrepareValueBytes<K, V> prepareValueBytes = this.prepareValueBytes;
        if ((prepareValueBytes == null && defaultValueProvider() != null) ||
                !valueBuilder.constantSizeMarshaller())
            return null;
        if (prepareValueBytes == null)
            prepareValueBytes = new ZeroOutValueBytes<>(valueSize());
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
    public ChronicleMapBuilder<K, V> replication(SingleChronicleHashReplication replication) {
        this.singleHashReplication = replication;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> replication(byte identifier) {
        return replication(SingleChronicleHashReplication.builder().createWithId(identifier));
    }

    @Override
    public ChronicleMapBuilder<K, V> replication(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        return replication(SingleChronicleHashReplication.builder()
                .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier));
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> instance() {
        return new MapInstanceConfig<>(this.clone(), singleHashReplication, null, null, null,
                new AtomicBoolean(false));
    }

    @Override
    public ChronicleMap<K, V> createStatelessClient(InetSocketAddress serverAddress)
            throws IOException {
        return statelessClient(serverAddress).create();
    }

    ChronicleMap<K, V> createStatelessClient(StatelessMapConfig<K, V> statelessClientConfig)
            throws IOException {
        pushingToMapEventListener();
        preMapConstruction(false);
        return new StatelessChronicleMap<>(statelessClientConfig, this);
    }

    @Override
    public ChronicleMap<K, V> createPersistedTo(File file) throws IOException {
        // clone() to make this builder instance thread-safe, because createWithFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithFile(file, singleHashReplication, null);
    }

    @Override
    public StatelessClientConfig<ChronicleMap<K, V>> statelessClient(
            InetSocketAddress remoteAddress) {
        return new StatelessMapConfig<>(clone(), remoteAddress, DEFAULT_STATELESS_CLIENT_TIMEOUT,
                null, new AtomicBoolean(false));
    }

    @Override
    public ChronicleMap<K, V> create() {
        // clone() to make this builder instance thread-safe, because createWithoutFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithoutFile(singleHashReplication, null);
    }

    ChronicleMap<K, V> create(MapInstanceConfig<K, V> ib) throws IOException {
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

    ChronicleMap<K, V> createWithoutFile(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel) {
        try {
            pushingToMapEventListener();
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
            Class<?> pmel = Class.forName(
                    "com.higherfrequencytrading.chronicle.enterprise.map.PushingMapEventListener");
            Constructor<?> constructor = pmel.getConstructor(ChronicleMap[].class);
            // create a stateless client for each address
            ChronicleMap[] statelessClients = new ChronicleMap[pushToAddresses.length];
            ChronicleMapBuilder<K, V> cmb = clone();
            cmb.pushTo((InetSocketAddress[]) null);
            for (int i = 0; i < pushToAddresses.length; i++) {
                statelessClients[i] = cmb.createStatelessClient(pushToAddresses[i]);
            }
            eventListener =
                    (MapEventListener<K, V>) constructor.newInstance((Object) statelessClients);
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(getClass().getName())
                    .warn("Chronicle Enterprise not found in the class path");
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass().getName())
                    .error("PushingMapEventListener failed to load", e);
        }
    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel) throws IOException {
        boolean replicated = singleHashReplication != null || channel != null;
        preMapConstruction(replicated);
        if (replicated) {
            AbstractReplication replication;
            if (singleHashReplication != null) {
                replication = singleHashReplication;
            } else {
                replication = channel.hub();
            }
            return new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesInterop<V, Object>>(this, replication);
        } else {
            return new VanillaChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesInterop<V, Object>>(this);
        }
    }

    void preMapConstruction(boolean replicated) {
        keyBuilder.objectSerializer(acquireObjectSerializer(JDKObjectSerializer.INSTANCE));
        valueBuilder.objectSerializer(acquireObjectSerializer(JDKObjectSerializer.INSTANCE));

        long maxSize = (long) entrySize(replicated) * figureBufferAllocationFactor();
        keyBuilder.maxSize(maxSize);
        valueBuilder.maxSize(maxSize);

        if (sampleKey != null)
            keyBuilder.constantSizeBySample(sampleKey);
        if (sampleValue != null)
            valueBuilder.constantSizeBySample(sampleValue);
        stateChecks();
    }

    private void stateChecks() {
        checkAlignmentOnlyIfValuesPossiblyReferenceOffHeap();
        checkPrepareValueBytesOnlyIfConstantValueSize();
    }

    private ChronicleMap<K, V> establishReplication(
            VanillaChronicleMap<K, ?, ?, V, ?, ?> map,
            SingleChronicleHashReplication singleHashReplication,
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
}

