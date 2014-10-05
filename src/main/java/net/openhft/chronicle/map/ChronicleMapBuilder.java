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

package net.openhft.chronicle.map;

import net.openhft.chronicle.map.serialization.MetaBytesInterop;
import net.openhft.chronicle.map.serialization.MetaBytesWriter;
import net.openhft.chronicle.map.serialization.MetaProvider;
import net.openhft.chronicle.map.threadlocal.Provider;
import net.openhft.chronicle.map.threadlocal.ThreadLocalCopies;
import net.openhft.lang.Maths;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.Objects.builderEquals;

public class ChronicleMapBuilder<K, V> implements Cloneable {

    private static final Bytes EMPTY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;

    public static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapBuilder.class.getName());
    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;
    boolean transactional = false;
    Replicator firstReplicator;
    Map<Class<? extends Replicator>, Replicator> replicators = new HashMap<Class<? extends Replicator>, Replicator>();
    boolean forceReplicatedImpl = false;
    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;
    private int keySize = 0;
    private int valueSize = 0;
    private int entrySize = 0;
    private Alignment alignment = Alignment.OF_4_BYTES;
    private long entries = 1 << 20;
    private int replicas = 0;
    private long lockTimeOut = 2000;
    private TimeUnit lockTimeOutUnit = TimeUnit.MILLISECONDS;
    private int metaDataBytes = 0;
    private MapErrorListener errorListener = MapErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean largeSegments = false;
    // replication
    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;
    private MapEventListener<K, V, ChronicleMap<K, V>> eventListener =
            MapEventListeners.nop();
    private V defaultValue = null;
    private DefaultValueProvider<K, V> defaultValueProvider = null;

    ChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<K>(keyClass, SerializationBuilder.Role.KEY);
        valueBuilder = new SerializationBuilder<V>(valueClass, SerializationBuilder.Role.VALUE);
    }

    public static <K, V> ChronicleMapBuilder<K, V> of(Class<K> keyClass, Class<V> valueClass) {
        return new ChronicleMapBuilder<K, V>(keyClass, valueClass);
    }

    private static long roundUpMapHeaderSize(long headerSize) {
        long roundUp = (headerSize + 127L) & ~127L;
        if (roundUp - headerSize < 64)
            roundUp += 128;
        return roundUp;
    }

    @Override
    public ChronicleMapBuilder<K, V> clone() {
        try {
            @SuppressWarnings("unchecked")
            final ChronicleMapBuilder<K, V> result = (ChronicleMapBuilder<K, V>) super.clone();
            result.keyBuilder = keyBuilder.clone();
            result.valueBuilder = valueBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Set minimum number of segments. See concurrencyLevel
     * in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @param minSegments the minimum number of segments in maps, constructed by this builder
     * @return this builder object back
     */
    public ChronicleMapBuilder<K, V> minSegments(int minSegments) {
        this.minSegments = minSegments;
        return this;
    }

    public int minSegments() {
        return minSegments < 1 ? tryMinSegments(4, 65536) : minSegments;
    }

    private int tryMinSegments(int min, int max) {
        for (int i = min; i < max; i <<= 1) {
            if (i * i * i >= alignedEntrySize() * 2)
                return i;
        }
        return max;
    }

    /**
     * Configures the number of bytes, taken by serialized form of keys, put into maps, created by
     * this builder.
     *
     * <p>If key is a boxed primitive type or {@link Byteable} subclass, i. e. if key size is known
     * statically, it is automatically accounted and shouldn't be specified by user.
     *
     * <p>If key size varies moderately, specify the size higher than average, but lower than
     * the maximum possible, to minimize average memory overuse. If key size varies in a wide range,
     * it's better to use {@link #entrySize() entry size} in "chunk" mode and configure it directly.
     *
     * <p>Example: if keys in your map(s) are English words in {@link String} form, keys size 10
     * (a bit more than average English word length) would be a good choice: <pre>{@code
     * ChronicleMap<String, LongValue> wordFrequencies = ChronicleMapBuilder
     *     .of(String.class, directClassFor(LongValue.class))
     *     .entries(50000)
     *     .keySize(10)
     *     // shouldn't specify valueSize(), because it is statically known
     *     .create();}</pre>
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded
     * (and each character takes 2 bytes on-heap), because default off-heap {@link String} encoding
     * is UTF-8 in {@code ChronicleMap}.)
     *
     * @param keySize number of bytes, taken by serialized form of keys
     * @return this {@code ChronicleMapBuilder} back
     * @see #valueSize(int)
     * @see #entrySize()
     */
    public ChronicleMapBuilder<K, V> keySize(int keySize) {
        if (keySize <= 0)
            throw new IllegalArgumentException("Key size must be positive");
        this.keySize = keySize;
        return this;
    }

    private int keySize() {
        return keyOrValueSize(keySize, keyBuilder);
    }

    /**
     * Configures the number of bytes, taken by serialized form of value, put into maps, created by
     * this builder.
     *
     * <p>If value is a boxed primitive type or {@link Byteable} subclass, i. e. if value size
     * is known statically, it is automatically accounted and shouldn't be specified by user.
     *
     * <p>If value size varies moderately, specify the size higher than average, but lower than
     * the maximum possible, to minimize average memory overuse. If value size varies in a wide
     * range, it's better to use {@link #entrySize() entry size} in "chunk" mode and configure
     * it directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this {@code ChronicleMapBuilder} back
     * @see #keySize(int)
     * @see #entrySize()
     */
    public ChronicleMapBuilder<K, V> valueSize(int valueSize) {
        if (valueSize <= 0)
            throw new IllegalArgumentException("Value size must be positive");
        this.valueSize = valueSize;
        return this;
    }

    private int valueSize() {
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
     * Configures the {@linkplain #entrySize() entry size}.
     *
     * <p>Note that the actual entrySize will be aligned to 4 (default {@linkplain
     * #entryAndValueAlignment() entry alignment}). I. e. if you set entry size to 30, the actual
     * entry size will be 32 (30 aligned to 4 bytes). If you don't want entry size to be aligned,
     * set {@code entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @param entrySize the size in bytes
     * @return this {@code ChronicleMapBuilder} back
     * @see #entrySize()
     * @see #entryAndValueAlignment(Alignment)
     * @see #entryAndValueAlignment()
     */
    public ChronicleMapBuilder<K, V> entrySize(int entrySize) {
        if (entrySize <= 0)
            throw new IllegalArgumentException("Entry Size must be positive");
        this.entrySize = entrySize;
        return this;
    }

    /**
     * Returns the size in bytes of allocation unit of {@code ChronicleMap} instances, created by
     * this builder.
     *
     * <p>{@code ChronicleMap} stores it's data off-heap, so it is required to serialize
     * key and values (unless they are direct {@link Byteable} instances). Serialized key bytes +
     * serialized value bytes + some metadata bytes comprise "entry space", which
     * {@code ChronicleMap} should allocate. So <i>entry size</i> is a minimum allocation portion
     * in the maps, created by this builder. E. g. if entry size is 100, the created map could only
     * allocate 100, 200, 300... bytes for an entry. If say 150 bytes of entry space are required
     * by the entry, 200 bytes will be allocated, 150 used and 50 wasted. To minimize memory overuse
     * and improve speed, you should pay decent attention to this configuration.
     *
     * <p>In fully default case you can expect entry size to be about 256 bytes. But it is strongly
     * recommended always to configure {@linkplain #keySize(int) key size} and
     * {@linkplain #valueSize(int) value size}, if they couldn't be derived statically.
     *
     * <p>If entry size is not {@linkplain #entrySize(int) configured} explicitly, it is computed
     * based on {@linkplain #metaDataBytes() meta data bytes}, plus {@linkplain #keySize(int) key
     * size}, plus {@linkplain #valueSize(int) value size}, plus a few bytes required by
     * implementations, with respect to {@linkplain #entryAndValueAlignment() alignment}.
     *
     * <p>There are three major patterns of this configuration usage:
     * <ul>
     *     <li>Key and value sizes are known exactly. Just specify them using precisely
     *     corresponding methods. No memory would be wasted in this case.</li>
     *     <li>Key and/or value size varies moderately. Specify them using corresponding methods,
     *     or {@linkplain #entrySize(int) specify entry size} directly, by values somewhere between
     *     average and maximum possible. The idea is to have most (90% or more) entries to fit
     *     a single "entry size" with moderate memory waste (10-20% on average), rest 10% or less of
     *     entries should take 2 "entry sizes", thus with ~50% memory overuse.</li>
     *     <li>Key and/or value size varies in a wide range. Then it's best to use entry size
     *     configuration in <i>chunk mode</i>. Specify entry size so that most entries should take
     *     from 5 to several dozens of "chunks". With this approach, average memory waste should be
     *     very low.
     *
     *     <p>However, remember that operations with entries that span several "entry sizes"
     *     are a bit slower, than entries which take a single "entry size" (that is why "chunk"
     *     approach is not recommended, when key and/or value size varies moderately). Also, note
     *     that the maximum number of chunks could be taken by an entry is 64.
     *     {@link IllegalArgumentException} is thrown on attempt to insert too large entry, compared
     *     to the configured or computed entry size.
     *
     *     <p>Example: if values in your map are adjacency lists of some social graph, where nodes
     *     are represented as {@code long} ids, and adjacency lists are serialized in efficient
     *     manner, for example as {@code long[]} arrays. Typical number of connections is 100-300,
     *     maximum is 3000. In this case entry size of 50 * (8 bytes for each id) = 400 bytes would
     *     be a good choice: <pre>{@code
     * Map<Long, long[]> socialGraph = ChronicleMapBuilder
     *     .of(Long.class, long[].class)
     *     .entries(1_000_000_000)
     *     .entrySize(50 * 8)
     *     .create();}</pre>
     *     It is minimum possible (because 3000 friends / 50 friends = 60 is close to 64 "max
     *     chunks by single entry" limit, and ensures moderate average memory overuse (not more
     *     than 20%).
     *     </li>
     * </ul>
     *
     * @return size of memory allocation unit (in bytes)
     */
    public int entrySize() {
        if (entrySize > 0)
            return entrySize;
        int size = metaDataBytes;
        int keySize = keySize();
        size += keyBuilder.sizeMarshaller().sizeEncodingSize(keySize);
        size += keySize;
        if (replicatedImpl())
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

    int alignedEntrySize() {
        return entryAndValueAlignment().alignSize(entrySize());
    }

    /**
     * Configures alignment strategy of address in memory of entries and independently of address
     * in memory of values within entries in ChronicleMaps, created by this builder.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with
     * volatile access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>Note that {@link #entrySize()} will be aligned according to this alignment.
     * I. e. if you set {@code entrySize(20)} and {@link Alignment#OF_8_BYTES}, actual entry size
     * will be 24 (20 aligned to 8 bytes).
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public ChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    /**
     * Returns alignment strategy of addresses in memory of entries and independently of values
     * within entries in ChronicleMaps, created by this builder.
     *
     * <p>Default is {@link Alignment#OF_4_BYTES}.
     *
     * @return entry/value alignment strategy
     * @see #entryAndValueAlignment(Alignment)
     */
    public Alignment entryAndValueAlignment() {
        return alignment;
    }

    public ChronicleMapBuilder<K, V> entries(long entries) {
        this.entries = entries;
        return this;
    }

    public long entries() {
        return entries;
    }

    public ChronicleMapBuilder<K, V> replicas(int replicas) {
        this.replicas = replicas;
        return this;
    }

    public int replicas() {
        return replicas;
    }

    public ChronicleMapBuilder<K, V> actualEntriesPerSegment(int actualEntriesPerSegment) {
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
    }

    public int actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0)
            return actualEntriesPerSegment;
        int as = actualSegments();
        // round up to the next multiple of 64.
        return (int) (Math.max(1, entries * 2L / as) + 63) & ~63;
    }

    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        this.actualSegments = actualSegments;
        return this;
    }

    public int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        if (!largeSegments && entries > (long) minSegments() << 15) {
            long segments = Maths.nextPower2(entries >> 15, 128);
            if (segments < 1 << 20)
                return (int) segments;
        }
        // try to keep it 16-bit sizes segments
        return (int) Maths.nextPower2(Math.max((entries >> 30) + 1, minSegments()), 1);
    }

    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code ChronicleMapBuilder} back
     */
    public ChronicleMapBuilder<K, V> transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }

    public ChronicleMapBuilder<K, V> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        this.lockTimeOut = lockTimeOut;
        lockTimeOutUnit = unit;
        return this;
    }

    public long lockTimeOut(TimeUnit unit) {
        return unit.convert(lockTimeOut, lockTimeOutUnit);
    }

    public ChronicleMapBuilder<K, V> errorListener(MapErrorListener errorListener) {
        this.errorListener = errorListener;
        return this;
    }

    public MapErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want ChronicleMap.put() to not return the object that was replaced
     *                       but instead return null
     * @return an instance of the map builder
     */
    public ChronicleMapBuilder<K, V> putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if ChronicleMap.put() is not going to return the object that was replaced but instead
     * return null
     */
    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want ChronicleMap.remove() to not return the object that was
     *                          removed but instead return null
     * @return an instance of the map builder
     */
    public ChronicleMapBuilder<K, V> removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    /**
     * Map.remove() returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @return true if ChronicleMap.remove() is not going to return the object that was removed but instead
     * return null
     */
    public boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    public boolean largeSegments() {
        return entries > 1L << (20 + 15) || largeSegments;
    }

    public ChronicleMapBuilder<K, V> largeSegments(boolean largeSegments) {
        this.largeSegments = largeSegments;
        return this;
    }

    public ChronicleMapBuilder<K, V> metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return this;
    }

    public int metaDataBytes() {
        return metaDataBytes;
    }

    @Override
    public String toString() {
        return "ChronicleMapBuilder{" +
                "actualSegments=" + actualSegments() +
                ", minSegments=" + minSegments() +
                ", actualEntriesPerSegment=" + actualEntriesPerSegment() +
                ", keySize=" + keySize() +
                ", valueSize=" + valueSize() +
                ", entrySize=" + entrySize() +
                ", entryAndValueAlignment=" + entryAndValueAlignment() +
                ", entries=" + entries() +
                ", replicas=" + replicas() +
                ", transactional=" + transactional() +
                ", lockTimeOut=" + lockTimeOut + " " + lockTimeOutUnit +
                ", metaDataBytes=" + metaDataBytes() +
                ", errorListener=" + errorListener() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", largeSegments=" + largeSegments() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerfactory=" + bytesMarshallerFactory() +
                ", keyBuilder=" + keyBuilder +
                ", valueBuilder=" + valueBuilder +
                ", eventListener=" + eventListener +
                ", defaultValue=" + defaultValue +
                ", defaultValueProvider=" + defaultValueProvider +
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

    public ChronicleMapBuilder<K, V> addReplicator(Replicator replicator) {
        if (firstReplicator == null) {
            firstReplicator = replicator;
            replicators.put(replicator.getClass(), replicator);
        } else {
            if (replicator.identifier() != firstReplicator.identifier()) {
                throw new IllegalArgumentException(
                        "Identifiers of all replicators of the map should be the same");
            }
            if (replicators.containsKey(replicator.getClass())) {
                throw new IllegalArgumentException("Replicator of " + replicator.getClass() +
                        " class has already to the map");
            }
            replicators.put(replicator.getClass(), replicator);
        }
        return this;
    }

    public ChronicleMapBuilder<K, V> setReplicators(Replicator... replicators) {
        firstReplicator = null;
        this.replicators.clear();
        for (Replicator replicator : replicators) {
            addReplicator(replicator);
        }
        return this;
    }

    public ChronicleMapBuilder<K, V> timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    public TimeProvider timeProvider() {
        return timeProvider;
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return bytesMarshallerFactory == null ?
                bytesMarshallerFactory = new VanillaBytesMarshallerFactory() :
                bytesMarshallerFactory;
    }

    public ChronicleMapBuilder<K, V> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    public ObjectSerializer objectSerializer() {
        return objectSerializer == null ?
                objectSerializer = BytesMarshallableSerializer.create(
                        bytesMarshallerFactory(), JDKObjectSerializer.INSTANCE) :
                objectSerializer;
    }

    public ChronicleMapBuilder<K, V> objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return this;
    }

    /**
     * For testing
     */
    ChronicleMapBuilder<K, V> forceReplicatedImpl() {
        this.forceReplicatedImpl = true;
        return this;
    }

    public ChronicleMapBuilder<K, V> keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller) {
        keyBuilder.marshaller(keyMarshaller, null);
        return this;
    }

    public ChronicleMapBuilder<K, V> valueMarshallerAndFactory(
            @NotNull BytesMarshaller<V> valueMarshaller, @NotNull ObjectFactory<V> valueFactory) {
        valueBuilder.marshaller(valueMarshaller, valueFactory);
        return this;
    }

    public ChronicleMapBuilder<K, V> valueFactory(@NotNull ObjectFactory<V> valueFactory) {
        valueBuilder.factory(valueFactory);
        return this;
    }

    public ChronicleMapBuilder<K, V> eventListener(
            MapEventListener<K, V, ChronicleMap<K, V>> eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public MapEventListener<K, V, ChronicleMap<K, V>> eventListener() {
        return eventListener;
    }

    /**
     * Specifies the value to be put for each key queried in {@link ChronicleMap#get get()} and
     * {@link ChronicleMap#getUsing(Object, Object) getUsing()} methods, if the key is absent
     * in the map. Then this default value is returned from query method.
     *
     * <p>Setting default value to {@code null} is interpreted as map shouldn't put any default
     * value for absent keys. This is by default.
     *
     * <p>This configuration overrides any previous
     * {@link #defaultValueProvider(DefaultValueProvider)} configuration to this
     * {@code ChronicleMapBuilder}.
     *
     * @param defaultValue the default value to be put to the map for absent keys during
     * {@code get()} and {@code getUsing()} calls and returned from these calls
     * @return this builder object back
     */
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        this.defaultValueProvider = null;
        return this;
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#get get()}
     * and {@link ChronicleMap#getUsing(Object, Object) getUsing()} calls, if the key is absent
     * in the map. If the obtained value is non-null, it is put for the key in the map and then
     * returned from current {@code get()} or {@code getUsing()} call.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} configuration
     * to this {@code ChronicleMapBuilder}.
     *
     * @param defaultValueProvider the strategy to obtain a default value by the absent key
     * @return this builder object back
     */
    public ChronicleMapBuilder<K, V> defaultValueProvider(
            DefaultValueProvider<K, V> defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        return this;
    }

    /**
     * Non-public because should be called only after {@link #preMapConstruction()}
     */
    DefaultValueProvider<K, V> defaultValueProvider() {
        if (defaultValueProvider != null)
            return defaultValueProvider;
        if (defaultValue == null)
            return NullValueProvider.INSTANCE;
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

    public ChronicleMap<K, V> create(File file) throws IOException {
        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                FileInputStream fis = new FileInputStream(file);
                ObjectInputStream ois = new ObjectInputStream(fis);
                try {
                    VanillaChronicleMap<K, ?, ?, V, ?, ?> map =
                            (VanillaChronicleMap<K, ?, ?, V, ?, ?>) ois.readObject();
                    map.headerSize = roundUpMapHeaderSize(fis.getChannel().position());
                    map.createMappedStoreAndSegments(file);
                    return establishReplication(map);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                } finally {
                    ois.close();
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

        VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap();

        FileOutputStream fos = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        try {
            oos.writeObject(map);
            oos.flush();
            map.headerSize = roundUpMapHeaderSize(fos.getChannel().position());
            map.createMappedStoreAndSegments(file);
        } finally {
            oos.close();
        }

        return establishReplication(map);
    }

    public ChronicleMap<K, V> create() throws IOException {
        VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap();
        BytesStore bytesStore = new DirectStore(objectSerializer(), map.sizeInBytes(), true);
        map.createMappedStoreAndSegments(bytesStore);
        return establishReplication(map);
    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap() throws IOException {
        preMapConstruction();
        if (!replicatedImpl()) {
            return new VanillaChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        } else {
            return new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        }
    }

    private boolean replicatedImpl() {
        return firstReplicator != null || forceReplicatedImpl;
    }

    private void preMapConstruction() {
        keyBuilder.objectSerializer(objectSerializer());
        valueBuilder.objectSerializer(objectSerializer());

        int maxSize = entrySize() * figureBufferAllocationFactor();
        keyBuilder.maxSize(maxSize);
        valueBuilder.maxSize(maxSize);
    }

    private ChronicleMap<K, V> establishReplication(ChronicleMap<K, V> map)
            throws IOException {
        if (map instanceof ReplicatedChronicleMap) {
            ReplicatedChronicleMap result = (ReplicatedChronicleMap) map;
            for (Replicator replicator : replicators.values()) {
                Closeable token = replicator.applyTo(this, result, result);
                if (replicators.size() == 1 && token.getClass() == UdpReplicator.class) {
                    LOG.warn(
                            "MISSING TCP REPLICATION : The UdpReplicator only attempts to read data " +
                                    "(it does not enforce or guarantee delivery), you should use" +
                                    "the UdpReplicator if you have a large number of nodes, and you wish" +
                                    "to receive the data before it becomes available on TCP/IP. Since data" +
                                    "delivery is not guaranteed, it is recommended that you only use" +
                                    "the UDP Replicator in conjunction with a TCP Replicator"
                    );
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

