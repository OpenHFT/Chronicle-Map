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
import net.openhft.chronicle.hash.serialization.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.MetaBytesWriter;
import net.openhft.chronicle.hash.serialization.MetaProvider;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.Objects.builderEquals;

/**
 * {@code AbstractChronicleMapBuilder} manages most of {@link ChronicleMap} configurations; has two
 * concrete subclasses: {@link ChronicleMapBuilder} should be used to create maps with ordinary
 * values, {@link OffHeapUpdatableChronicleMapBuilder} -- maps with {@link Byteable} values, which
 * point directly to off-heap memory; could be used as a classic builder and/or factory. This means
 * that in addition to the standard builder usage pattern: <pre>{@code
 * ChronicleMap<Key, Value> map = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500)
 *     // ... other configurations
 *     .create();}</pre>
 * one of concrete {@code AbstractChronicleMapBuilder} subclasses, {@link ChronicleMapBuilder} or
 * {@link OffHeapUpdatableChronicleMapBuilder}, could be prepared and used to create many similar
 * maps: <pre>{@code
 * OffHeapUpdatableChronicleMapBuilder<Key, Value> builder = OffHeapUpdatableChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500);
 *
 * ChronicleMap<Key, Value> map1 = builder.create();
 * ChronicleMap<Key, Value> map2 = builder.create();}</pre>
 * i. e. created {@code ChronicleMap} instances don't depend on the builder.
 *
 * <p>{@code AbstractChronicleMapBuilder} and it's subclasses are mutable, see a note in
 * {@link ChronicleHashBuilder} interface documentation.
 *
 * <p>Later in this documentation, "ChronicleMap" means "ChronicleMaps, created by {@code
 * AbstractChronicleMapBuilder}", unless specified different, because theoretically someone might
 * provide {@code ChronicleMap} implementations with completely different properties.
 *
 * <p>{@code ChronicleMap} ("ChronicleMaps, created by {@code AbstractChronicleMapBuilder}")
 * currently doesn't support resizing. That is why you should <i>always</i> configure {@linkplain
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
public abstract class AbstractChronicleMapBuilder<K, V,
        B extends AbstractChronicleMapBuilder<K, V, B>>
        implements Cloneable, ChronicleHashBuilder<K, ChronicleMap<K, V>, B> {

    private static final Bytes EMPTY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;

    private static final int MAX_SEGMENTS = (1 << 30);
    private static final int MAX_SEGMENTS_TO_CHAISE_COMPACT_MULTI_MAPS = (1 << 20);

    static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractChronicleMapBuilder.class.getName());


    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;

    private Map<Class<? extends Replicator>, Replicator> replicators = new HashMap<Class<? extends Replicator>, Replicator>();

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
    private Alignment alignment = Alignment.NO_ALIGNMENT;
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
    private DefaultValueProvider<K, V> defaultValueProvider = NullValueProvider.INSTANCE;
    private PrepareValueBytes<K> prepareValueBytes = null;
    private byte identifier = -1;

    private StatelessBuilder statelessBuilder;
    private File file;

    AbstractChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<K>(keyClass, SerializationBuilder.Role.KEY);
        valueBuilder = new SerializationBuilder<V>(valueClass, SerializationBuilder.Role.VALUE);
    }


    private static long roundUpMapHeaderSize(long headerSize) {
        long roundUp = (headerSize + 127L) & ~127L;
        if (roundUp - headerSize < 64)
            roundUp += 128;
        return roundUp;
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
     * ChronicleMap<String, LongValue> wordFrequencies = OffHeapUpdatableChronicleMapBuilder
     *     .of(String.class, LongValue.class)
     *     .entries(50000)
     *     .keySize(10)
     *     // shouldn't specify valueSize(), because it is statically known
     *     .create();}</pre>
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded (and
     * each character takes 2 bytes on-heap), because default off-heap {@link String} encoding is
     * UTF-8 in {@code ChronicleMap}.)
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

    B valueSize(int valueSize) {
        if (valueSize <= 0)
            throw new IllegalArgumentException("Value size must be positive");
        this.valueSize = valueSize;
        return self();
    }

    B constantValueSizeBySample(V sampleValue) {
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
     * recommended always to configure {@linkplain #keySize(int) key size} and {@linkplain
     * #valueSize(int) value size}, if they couldn't be derived statically.
     *
     * <p>If entry size is not configured explicitly by calling this method, it is computed based on
     * {@linkplain #metaDataBytes(int) meta data bytes}, plus {@linkplain #keySize(int) key size},
     * plus {@linkplain #valueSize(int) value size}, plus a few bytes required by implementations.
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

    int entrySize() {
        if (entrySize > 0)
            return entryAndValueAlignment().alignSize(entrySize);
        int size = metaDataBytes;
        int keySize = keySize();
        size += keyBuilder.sizeMarshaller().sizeEncodingSize(keySize);
        size += keySize;
        if (useReplication())
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

    B entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return self();
    }

    Alignment entryAndValueAlignment() {
        return alignment;
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
                    VanillaIntIntMultiMap.MAX_CAPACITY + ", " + actualEntriesPerSegment + " given");
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return self();
    }

    private boolean tooManyEntriesPerSegment(long entriesPerSegment) {
        return entriesPerSegment > VanillaIntIntMultiMap.MAX_CAPACITY;
    }

    long actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0L)
            return actualEntriesPerSegment;
        int actualSegments = actualSegments();
        long actualEntries = totalEntriesIfPoorDistribution(actualSegments);
        long actualEntriesPerSegment = divideUpper(actualEntries, actualSegments);
        if (tooManyEntriesPerSegment(actualEntriesPerSegment))
            throw new IllegalStateException("max entries per segment is " +
                    VanillaIntIntMultiMap.MAX_CAPACITY + " configured entries() and " +
                    "actualSegments() so that there should be " + actualEntriesPerSegment +
                    " entries per segment");
        return actualEntriesPerSegment;
    }

    private long totalEntriesIfPoorDistribution(int segments) {
        if (segments == 1)
            return entries;
        double poorDistEntriesScale = Math.log(segments) * entries;
        if (segments <= 8)
            return Math.min(entries * segments,
                    (long) (entries  + poorDistEntriesScale * 0.08 + 64)); // 8% was min for tests
        return (long) (entries  + poorDistEntriesScale * 0.11 + 80); // 11% was min for tests
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
        for (int power = 0; power < 7; power++) {
            if (entries < (1L << (7 + (2 * power))))
                return 1 << power;
        }
        // Heuristic -- number of segments ~= cube root from number of entries seems optimal
        int maxSegments = 65536;
        for (int segments = 4; segments < maxSegments; segments <<= 1) {
            if (((long) segments) * segments * segments >= entries)
                return segments;
        }
        return maxSegments;
    }

    @Override
    public B actualSegments(int actualSegments) {
        checkSegments(actualSegments);
        this.actualSegments = actualSegments;
        return self();
    }

    private static void checkSegments(int segments) {
        if (segments <= 0 || segments > MAX_SEGMENTS)
            throw new IllegalArgumentException("segments should be positive, " +
                    segments + " given");
        if (segments > MAX_SEGMENTS)
            throw new IllegalArgumentException("Max segments is " + MAX_SEGMENTS + ", " +
                    segments + " given");
    }

    int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        long shortMMapSegments = trySegments(VanillaShortShortMultiMap.MAX_CAPACITY,
                MAX_SEGMENTS_TO_CHAISE_COMPACT_MULTI_MAPS);
        if (shortMMapSegments > 0L)
            return (int) shortMMapSegments;
        long intMMapSegments = trySegments(VanillaIntIntMultiMap.MAX_CAPACITY, MAX_SEGMENTS);
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

    private static long divideUpper(long dividend, long divisor) {
        return ((dividend - 1L) / divisor) + 1L;
    }

    private boolean canSupportShortShort() {
        return entries > (long) minSegments() << 15;
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
     * Configures if the maps created by this builder should return {@code null} instead of previous
     * mapped values on {@link ChronicleMap#put(Object, Object) ChornicleMap.put(key, value)}
     * calls.
     *
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality which
     * is rarely used but fairly cheap for {@link HashMap}. In the case, for an off heap collection,
     * it has to create a new object and deserialize the data from off-heap memory. It's expensive
     * for something you probably don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract and
     * returns the previous mapped value on {@code put()} calls.
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but
     *                       instead return {@code null}
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
     * Configures if the maps created by this builder should return {@code null} instead of the last
     * mapped value on {@link ChronicleMap#remove(Object) ChronicleMap.remove(key)} calls.
     *
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is
     * rarely used but fairly cheap for {@link HashMap}. In the case, for an off heap collection, it
     * has to create a new object and deserialize the data from off-heap memory. It's expensive for
     * something you probably don't use.
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry
     *                          but instead return {@code null}
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
                "actualSegments=" + pretty(actualSegments) +
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

    private static String pretty(int value) {
        return value > 0 ? value + "" : "not configured";
    }

    private static String pretty(Object obj) {
        return obj != null ? obj + "" : "not configured";
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

    public B replicators(byte identifier, ReplicationConfig... replicationConfigs) {

        this.identifier = identifier;
        this.replicators.clear();
        for (ReplicationConfig replicationConfig : replicationConfigs) {

            Replicator replicator;
            if (replicationConfig instanceof TcpReplicationConfig) {
                replicator = Replicators.tcp((TcpReplicationConfig) replicationConfig);
            } else if (replicationConfig instanceof UdpReplicationConfig) {
                replicator = Replicators.udp((UdpReplicationConfig) replicationConfig);
            } else
                throw new UnsupportedOperationException();


            this.replicators.put(replicator.getClass(), replicator);
        }

        return self();
    }


    public B channel(ChannelProvider.ChronicleChannel chronicleChannel) {
        this.identifier = chronicleChannel.identifier();
        this.replicators.clear();
        replicators.put(chronicleChannel.getClass(), chronicleChannel);

        return self();
    }

    @Override
    public B disableReplication() {
        identifier = -1;
        replicators.clear();
        return self();
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
        keyBuilder.marshaller(keyMarshaller, null);
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
    public B stateless(@NotNull StatelessBuilder statelessBuilder) {
        this.statelessBuilder = statelessBuilder;
        return self();
    }

    @Override
    public B file(File file) throws IOException {
        this.file = file;
        return self();
    }

    /**
     * Configures factory which is used to create a new value instance, if value class is either
     * {@link Byteable}, {@link BytesMarshallable} or {@link Externalizable} subclass in maps,
     * created by this builder.
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
     * Specifies the value to be put for each key queried in {@link ChronicleMap#acquireUsing
     * acquireUsing()} method, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValueProvider(
     * DefaultValueProvider)} configuration to this {@code AbstractChronicleMapBuilder}.
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
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} configuration to
     * this {@code AbstractChronicleMapBuilder}.
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

    B prepareValueBytesOnAcquire(@NotNull PrepareValueBytes<K> prepareValueBytes) {
        this.prepareValueBytes = prepareValueBytes;
        this.defaultValue = null;
        this.defaultValueProvider = null;
        return self();
    }

    PrepareValueBytesAsWriter<K> prepareValueBytesAsWriter() {
        return new PrepareValueBytesAsWriter<>(prepareValueBytes, valueSize());
    }


    /**
     * Non-public because should be called only after {@link #preMapConstruction()}
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

    public ChronicleMap<K, V> createWithFile(File file) throws IOException {
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


    @Override
    public ChronicleMap<K, V> create() throws IOException {

        if (statelessBuilder != null)
            return createStatelessMap(statelessBuilder);

        return (file != null) ? createWithFile(file) : createWithoutFile();
    }

    private ChronicleMap<K, V> createWithoutFile() throws IOException {
        VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap();
        BytesStore bytesStore = new DirectStore(JDKObjectSerializer.INSTANCE,
                map.sizeInBytes(), true);
        map.createMappedStoreAndSegments(bytesStore);
        return establishReplication(map);
    }

    private ChronicleMap<K, V> createStatelessMap(StatelessBuilder statelessBuilder) throws IOException {
        preMapConstruction();

        final KeyValueSerializer<K, V> keyValueSerializer
                = new KeyValueSerializer<K, V>(keyBuilder, valueBuilder);

        final Class<K> kClass = keyBuilder.eClass;
        final Class<V> vClass = valueBuilder.eClass;

        return new StatelessChronicleMap<K, V>(
                keyValueSerializer,
                statelessBuilder,
                entrySize(),
                kClass,
                vClass);
    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap() throws IOException {
        preMapConstruction();
        if (useReplication()) {
            return new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        } else {
            return new VanillaChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        }
    }

    private boolean useReplication() {
        return identifier != -1;
    }

    void preMapConstruction() {
        keyBuilder.objectSerializer(objectSerializer());
        valueBuilder.objectSerializer(objectSerializer());

        long maxSize = (long) entrySize() * figureBufferAllocationFactor();
        keyBuilder.maxSize(maxSize);
        valueBuilder.maxSize(maxSize);

        if (sampleKey != null)
            keyBuilder.constantSizeBySample(sampleKey);
        if (sampleValue != null)
            valueBuilder.constantSizeBySample(sampleValue);
    }

    private ChronicleMap<K, V> establishReplication(VanillaChronicleMap<K, ?, ?, V, ?, ?>  map)
            throws IOException {
        if (map instanceof ReplicatedChronicleMap) {
            ReplicatedChronicleMap result = (ReplicatedChronicleMap) map;
            for (Replicator replicator : replicators.values()) {
                if (replicator instanceof ChannelProvider.ChronicleChannel) {
                    ChannelProvider.ChronicleChannel channel =
                            (ChannelProvider.ChronicleChannel) replicator;
                    if (entrySize() > channel.provider().maxEntrySize()) {
                        throw new IllegalArgumentException("During ChannelProviderBuilder setup, " +
                                "maxEntrySize=" + channel.provider().maxEntrySize() +
                                " was specified, but map with " +
                                "entrySize=" + entrySize() + " is attempted to apply" +
                                "to the replicator");
                    }
                }
                Closeable token = replicator.applyTo(this, result, result, map);
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

    public byte identifier() {
        return identifier;
    }
}

