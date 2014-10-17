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

import net.openhft.chronicle.ChronicleHashBuilder;
import net.openhft.chronicle.ChronicleHashErrorListener;
import net.openhft.chronicle.ChronicleHashErrorListeners;
import net.openhft.chronicle.TimeProvider;
import net.openhft.chronicle.map.serialization.AgileBytesMarshaller;
import net.openhft.chronicle.map.serialization.MetaBytesInterop;
import net.openhft.chronicle.map.serialization.MetaBytesWriter;
import net.openhft.chronicle.map.serialization.MetaProvider;
import net.openhft.chronicle.map.threadlocal.Provider;
import net.openhft.chronicle.map.threadlocal.ThreadLocalCopies;
import net.openhft.lang.Maths;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.*;
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
 * {@code ChronicleMapBuilder} manages the whole set of {@link ChronicleMap} configurations, could be used as
 * a classic builder and/or factory. This means that in addition to the standard builder
 * usage pattern: <pre>{@code
 * ChronicleMap<Key, Value> map = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500)
 *     // ... other configurations
 *     .create();}</pre>
 * {@code ChronicleMapBuilder} could be prepared and used to create many similar maps: <pre>{@code
 * ChronicleMapBuilder<Key, Value> builder = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(100500);
 *
 * ChronicleMap<Key, Value> map1 = builder.create();
 * ChronicleMap<Key, Value> map2 = builder.create();}</pre>
 * i. e. created {@code ChronicleMap} instances don't depend on the builder.
 *
 * <p>Use static {@link #of(Class, Class)} method to obtain a {@code ChronicleMapBuilder} instance.
 *
 * <p>{@code ChronicleMapBuilder} is mutable. Configuration methods mutate the builder and return <i>the
 * builder itself</i> back to support chaining pattern, rather than the builder copies with the corresponding
 * configuration changed. To make an independent configuration, {@linkplain #clone} the builder.
 *
 * <p>Later in this documentation, "ChronicleMap" means "ChronicleMaps, created by {@code
 * ChronicleMapBuilder}", unless specified different, because theoretically someone might provide {@code
 * ChronicleMap} implementations with completely different properties.
 *
 * <p>{@code ChronicleMap} ("ChronicleMaps, created by {@code ChronicleMapBuilder}") currently doesn't support
 * resizing. That is why you should <i>always</i> configure {@linkplain #entries(long) number of entries} you
 * are going to insert into the created map <i>at most</i>. See {@link #entries(long)} method documentation
 * for more information on this.
 *
 * <p>{@code ChronicleMap} allocates memory by equally sized chunks. This size is called {@linkplain
 * #entrySize(int) entry size}, you are strongly recommended to configure it to achieve least memory
 * consumption and best speed. See {@link #entrySize(int)} method documentation for more information on this.
 *
 * @param <K> key type of the maps, produced by this builder
 * @param <V> value type of the maps, produced by this builder
 */
public class ChronicleMapBuilder<K, V> implements Cloneable,
        ChronicleHashBuilder<K, ChronicleMap<K, V>, ChronicleMapBuilder<K, V>> {

    private static final Bytes EMPTY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;

    public static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapBuilder.class.getName());

    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;

    private Map<Class<? extends Replicator>, Replicator> replicators = new HashMap<Class<? extends Replicator>, Replicator>();

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;
    private int keySize = 0;
    private K sampleKey;
    private int valueSize = 0;
    private V sampleValue;
    private int entrySize = 0;
    private Alignment alignment = Alignment.OF_4_BYTES;
    private long entries = 1 << 20;
    private long lockTimeOut = 2000;
    private TimeUnit lockTimeOutUnit = TimeUnit.MILLISECONDS;
    private int metaDataBytes = 0;
    private ChronicleHashErrorListener errorListener = ChronicleHashErrorListeners.logging();
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
    private byte identifier = -1;

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


    @Override
    public ChronicleMapBuilder<K, V> minSegments(int minSegments) {
        this.minSegments = minSegments;
        return this;
    }

    int minSegments() {
        return minSegments < 1 ? tryMinSegments(4, 65536) : minSegments;
    }

    private int tryMinSegments(int min, int max) {
        for (int i = min; i < max; i <<= 1) {
            if (i * i * i >= entrySize() * 2)
                return i;
        }
        return max;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Example: if keys in your map(s) are English words in {@link String} form, keys size 10
     * (a bit more than average English word length) would be a good choice: <pre>{@code
     * ChronicleMap<String, LongValue> wordFrequencies = ChronicleMapBuilder
     *     .of(String.class, directClassFor(LongValue.class))
     *     .entries(50000)
     *     .keySize(10)
     *     // shouldn't specify valueSize(), because it is statically known
     *     .create();}</pre>
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded (and each
     * character takes 2 bytes on-heap), because default off-heap {@link String} encoding is UTF-8 in {@code
     * ChronicleMap}.)
     *
     * @see #constantKeySizeBySample(Object)
     * @see #valueSize(int)
     * @see #entrySize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> keySize(int keySize) {
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
     * Configures the optimal number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. If value size is always the same, call {@link #constantValueSizeBySample(Object)} method
     * instead of this one.
     *
     * <p>If value is a boxed primitive type or {@link Byteable} subclass, i. e. if value size is known
     * statically, it is automatically accounted and shouldn't be specified by user.
     *
     * <p>If value size varies moderately, specify the size higher than average, but lower than the maximum
     * possible, to minimize average memory overuse. If value size varies in a wide range, it's better to use
     * {@linkplain #entrySize(int) entry size} in "chunk" mode and configure it directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this {@code ChronicleMapBuilder} back
     * @see #keySize(int)
     * @see #entrySize(int)
     */
    public ChronicleMapBuilder<K, V> valueSize(int valueSize) {
        if (valueSize <= 0)
            throw new IllegalArgumentException("Value size must be positive");
        this.valueSize = valueSize;
        return this;
    }

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. This is done by providing the {@code sampleValue}, all values should take the same number
     * of bytes in serialized form, as this sample object.
     *
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is known
     * statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If value size varies, method {@link #valueSize(int)} or {@link #entrySize(int)} should be called
     * instead of this one.
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
     * {@inheritDoc}
     *
     * <p>In fully default case you can expect entry size to be about 256 bytes. But it is strongly
     * recommended always to configure {@linkplain #keySize(int) key size} and {@linkplain #valueSize(int)
     * value size}, if they couldn't be derived statically.
     *
     * <p>If entry size is not configured explicitly by calling this method, it is computed based on
     * {@linkplain #metaDataBytes(int) meta data bytes}, plus {@linkplain #keySize(int) key size}, plus
     * {@linkplain #valueSize(int) value size}, plus a few bytes required by implementations, with respect to
     * {@linkplain #entryAndValueAlignment(Alignment) alignment}.
     *
     * <p>Note that the actual entrySize will be aligned to 4 (default {@linkplain
     * #entryAndValueAlignment(Alignment) entry alignment}). I. e. if you set entry size to 30, the actual
     * entry size will be 32 (30 aligned to 4 bytes). If you don't want entry size to be aligned, set {@code
     * entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @see #entryAndValueAlignment(Alignment)
     * @see #entries(long)
     */
    @Override
    public ChronicleMapBuilder<K, V> entrySize(int entrySize) {
        if (entrySize <= 0)
            throw new IllegalArgumentException("Entry Size must be positive");
        this.entrySize = entrySize;
        return this;
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

    /**
     * Configures alignment strategy of address in memory of entries and independently of address in memory of
     * values within entries in ChronicleMaps, created by this builder.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with volatile access,
     * because it doesn't work well if the value crosses cache lines. Also, on some (nowadays rare)
     * architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>Note that {@linkplain #entrySize(int) entry size} will be aligned according to this alignment. I. e.
     * if you set {@code entrySize(20)} and {@link Alignment#OF_8_BYTES}, actual entry size will be 24 (20
     * aligned to 8 bytes).
     *
     * <p>Default is {@link Alignment#OF_4_BYTES}.
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapBuilder} back
     */
    public ChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    Alignment entryAndValueAlignment() {
        return alignment;
    }

    @Override
    public ChronicleMapBuilder<K, V> entries(long entries) {
        this.entries = entries;
        return this;
    }

    long entries() {
        return entries;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualEntriesPerSegment(int actualEntriesPerSegment) {
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
    }

    int actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0)
            return actualEntriesPerSegment;
        int as = actualSegments();
        long actualEntries =
                as == 1 ? entries + 1 :        // The extra 1 might not be needed.
                        as <= 4 ? entries * 10L / 9 :  // 10%, assumes you are trying to be tight.
                                entries * 5L / 4;      // 25%

        // round up to the next multiple of 64.
        return (int) (Math.max(1, actualEntries / as) + 63) & ~63;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        this.actualSegments = actualSegments;
        return this;
    }

    int actualSegments() {
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
     * Configures if the maps created by this builder should return {@code null} instead of previous mapped
     * values on {@link ChronicleMap#put(Object, Object) ChornicleMap.put(key, value)} calls.
     *
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality which is rarely
     * used but fairly cheap for {@link HashMap}. In the case, for an off heap collection, it has to create a
     * new object and deserialize the data from off-heap memory. It's expensive for something you probably
     * don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract and returns the
     * previous mapped value on {@code put()} calls.
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but instead return
     *                       {@code null}
     * @return an instance of the map builder back
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
     * Configures if the maps created by this builder should return {@code null} instead of the last mapped
     * value on {@link ChronicleMap#remove(Object) ChronicleMap.remove(key)} calls.
     *
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is rarely
     * used but fairly cheap for {@link HashMap}. In the case, for an off heap collection, it has to create a
     * new object and deserialize the data from off-heap memory. It's expensive for something you probably
     * don't use.
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry but
     *                          instead return {@code null}
     * @return an instance of the map builder back
     * @see #putReturnsNull(boolean)
     */
    public ChronicleMapBuilder<K, V> removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    boolean largeSegments() {
        return entries > 1L << (20 + 15) || largeSegments;
    }

    @Override
    public ChronicleMapBuilder<K, V> largeSegments(boolean largeSegments) {
        this.largeSegments = largeSegments;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
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
                ", largeSegments=" + (largeSegments ? "true" : "not configured") +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerFactory=" + pretty(bytesMarshallerFactory) +
                ", objectSerializer=" + pretty(objectSerializer) +
                ", keyBuilder=" + keyBuilder +
                ", valueBuilder=" + valueBuilder +
                ", eventListener=" + eventListener +
                ", defaultValue=" + defaultValue +
                ", defaultValueProvider=" + pretty(defaultValueProvider) +
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

    public ChronicleMapBuilder<K, V> replicators(byte identifier, ReplicationConfig... replicationConfigs) {

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

        return this;
    }


    public ChronicleMapBuilder<K, V> channel(ChannelProvider.ChronicleChannel chronicleChannel) {
        this.identifier = chronicleChannel.identifier();
        this.replicators.clear();
        replicators.put(chronicleChannel.getClass(), chronicleChannel);

        return this;
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
    public ChronicleMapBuilder<K, V> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    ObjectSerializer objectSerializer() {
        return objectSerializer == null ?
                objectSerializer = BytesMarshallableSerializer.create(
                        bytesMarshallerFactory(), JDKObjectSerializer.INSTANCE) :
                objectSerializer;
    }

    @Override
    public ChronicleMapBuilder<K, V> objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return this;
    }


    @Override
    public ChronicleMapBuilder<K, V> keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller) {
        keyBuilder.marshaller(keyMarshaller, null);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> immutableKeys() {
        keyBuilder.instancesAreMutable(false);
        return this;
    }

    public ChronicleMapBuilder<K, V> valueMarshallerAndFactory(
            @NotNull BytesMarshaller<V> valueMarshaller, @NotNull ObjectFactory<V> valueFactory) {
        valueBuilder.marshaller(valueMarshaller, valueFactory);
        return this;
    }

    public ChronicleMapBuilder<K, V> valueMarshallerAndFactory(
            @NotNull AgileBytesMarshaller<V> valueMarshaller,
            @NotNull ObjectFactory<V> valueFactory) {
        valueBuilder.agileMarshaller(valueMarshaller, valueFactory);
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

    MapEventListener<K, V, ChronicleMap<K, V>> eventListener() {
        return eventListener;
    }

    /**
     * Specifies the value to be put for each key queried in {@link ChronicleMap#get get()} and {@link
     * ChronicleMap#getUsing(Object, Object) getUsing()} methods, if the key is absent in the map. Then this
     * default value is returned from query method.
     *
     * <p>Setting default value to {@code null} is interpreted as map shouldn't put any default value for
     * absent keys. This is by default.
     *
     * <p>This configuration overrides any previous {@link #defaultValueProvider(DefaultValueProvider)}
     * configuration to this {@code ChronicleMapBuilder}.
     *
     * @param defaultValue the default value to be put to the map for absent keys during {@code get()} and
     *                     {@code getUsing()} calls and returned from these calls
     * @return this builder object back
     */
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        this.defaultValueProvider = null;
        return this;
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#get get()} and {@link
     * ChronicleMap#getUsing(Object, Object) getUsing()} calls, if the key is absent in the map. If the
     * obtained value is non-null, it is put for the key in the map and then returned from current {@code
     * get()} or {@code getUsing()} call.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} configuration to this {@code
     * ChronicleMapBuilder}.
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

    @Override
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

    @Override
    public ChronicleMap<K, V> create() throws IOException {
        VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap();
        BytesStore bytesStore = new DirectStore(JDKObjectSerializer.INSTANCE,
                map.sizeInBytes(), true);
        map.createMappedStoreAndSegments(bytesStore);
        return establishReplication(map);
    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap() throws IOException {
        preMapConstruction();
        if (!useReplication()) {
            return new VanillaChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        } else {
            return new ReplicatedChronicleMap<K, Object, MetaBytesInterop<K, Object>,
                    V, Object, MetaBytesWriter<V, Object>>(this);
        }
    }

    private boolean useReplication() {
        return identifier != -1;
    }

      void preMapConstruction() {
        keyBuilder.objectSerializer(objectSerializer());
        valueBuilder.objectSerializer(objectSerializer());

        int maxSize = entrySize() * figureBufferAllocationFactor();
        keyBuilder.maxSize(maxSize);
        valueBuilder.maxSize(maxSize);

        if (sampleKey != null)
            keyBuilder.constantSizeBySample(sampleKey);
        if (sampleValue != null)
            valueBuilder.constantSizeBySample(sampleValue);
    }

    private ChronicleMap<K, V> establishReplication(ChronicleMap<K, V> map)
            throws IOException {
        if (map instanceof ReplicatedChronicleMap) {
            ReplicatedChronicleMap result = (ReplicatedChronicleMap) map;
            for (Replicator replicator : replicators.values()) {
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

