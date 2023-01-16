/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.MemoryUnit;
import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.entry.ChecksumStrategy;
import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;
import net.openhft.chronicle.hash.impl.util.Throwables;
import net.openhft.chronicle.hash.impl.util.math.PoissonDistribution;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableReaderWriter;
import net.openhft.chronicle.hash.serialization.impl.MarshallableReaderWriter;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.hash.serialization.impl.TypedMarshallableReaderWriter;
import net.openhft.chronicle.map.internal.AnalyticsHolder;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.values.ValueModel;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Double.isNaN;
import static java.lang.Math.round;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static net.openhft.chronicle.core.Maths.*;
import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.*;
import static net.openhft.chronicle.hash.impl.SizePrefixedBlob.*;
import static net.openhft.chronicle.hash.impl.VanillaChronicleHash.throwRecoveryOrReturnIOException;
import static net.openhft.chronicle.hash.impl.util.FileIOUtils.readFully;
import static net.openhft.chronicle.hash.impl.util.FileIOUtils.writeFully;
import static net.openhft.chronicle.hash.impl.util.Objects.builderEquals;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.format;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.report;
import static net.openhft.chronicle.map.DefaultSpi.mapEntryOperations;
import static net.openhft.chronicle.map.DefaultSpi.mapRemoteOperations;
import static net.openhft.chronicle.map.VanillaChronicleMap.alignAddr;

/**
 * {@code ChronicleMapBuilder} manages {@link ChronicleMap} configurations; could be used as a
 * classic builder and/or factory. This means that in addition to the standard builder usage
 * pattern: <pre>{@code
 * ChronicleMap<Key, Value> map = ChronicleMapOnHeapUpdatableBuilder
 *     .of(Key.class, Value.class)
 *     // ... other configurations
 *     .create();}</pre>
 * it could be prepared and used to create many similar maps: <pre>{@code
 * ChronicleMapBuilder<Key, Value> builder = ChronicleMapBuilder
 *     .of(Key.class, Value.class)
 *     .entries(..)
 *     // ... other configurations
 * <p>
 * ChronicleMap<Key, Value> map1 = builder.create();
 * ChronicleMap<Key, Value> map2 = builder.create();}</pre>
 * i. e. created {@code ChronicleMap} instances don't depend on the builder.
 * <p>
 * <p>{@code ChronicleMapBuilder} is mutable, see a note in {@link ChronicleHashBuilder} interface
 * documentation.
 * <p>
 * <p>Later in this documentation, "ChronicleMap" means "ChronicleMaps, created by {@code
 * ChronicleMapBuilder}", unless specified different, because theoretically someone might provide
 * {@code ChronicleMap} implementations with completely different properties.
 * <p>
 * <p>In addition to the key and value types, you <i>must</i> configure {@linkplain #entries(long)
 * number of entries} you are going to insert into the created map <i>at most</i>. See {@link
 * #entries(long)} method documentation for more information on this.
 * <p>
 * <p>If you key or value type is not constantly sized and known to {@code ChronicleHashBuilder}, i.
 * e. it is not a boxed primitive, {@linkplain net.openhft.chronicle.values.Values value interface},
 * or {@link Byteable}, you <i>must</i> provide the {@code ChronicleHashBuilder} with some
 * information about you keys or values: if they are constantly-sized, call {@link
 * #constantKeySizeBySample(Object)}, otherwise {@link #averageKey(Object)} or {@link
 * #averageKeySize(double)} method, and accordingly for values.
 * <p>
 * <p><a name="jvm-configurations"></a>
 * There are some JVM-level configurations, which are not stored in the ChronicleMap's persistence
 * file (or the other way to say this: they are not parts of <a
 * href="https://github.com/OpenHFT/Chronicle-Map/tree/ea/spec">the Chronicle Map data store
 * specification</a>) and have to be configured explicitly for each created on-heap {@code
 * ChronicleMap} instance, even if it is a view of an existing Chronicle Map data store. On the
 * other hand, JVM-level configurations could be different for different views of the same Chronicle
 * Map data store. The list of JVM-level configurations:
 * <ul>
 * <li>{@link #name(String)}</li>
 * <li>{@link #putReturnsNull(boolean)}</li>
 * <li>{@link #removeReturnsNull(boolean)}</li>
 * <li>{@link #entryOperations(MapEntryOperations)}</li>
 * <li>{@link #mapMethods(MapMethods)}</li>
 * <li>{@link #defaultValueProvider(DefaultValueProvider)}</li>
 * </ul>
 *
 * @param <K> key type of the maps, produced by this builder
 * @param <V> value type of the maps, produced by this builder
 * @see ChronicleHashBuilder
 * @see ChronicleMap
 * @see ChronicleSetBuilder
 */
public final class ChronicleMapBuilder<K, V> implements
        ChronicleHashBuilder<K, ChronicleMap<K, V>,
                ChronicleMapBuilder<K, V>> {

    private static final int UNDEFINED_ALIGNMENT_CONFIG = -1;
    private static final int NO_ALIGNMENT = 1;

    /**
     * If want to increase this number, note {@link OldDeletedEntriesCleanupThread} uses array
     * to store all segment indexes -- so it could be current JVM max array size,
     * not Integer.MAX_VALUE (which is an obvious limitation, as many APIs and internals use int
     * type for representing segment index).
     * <p>
     * Anyway, unlikely anyone ever need more than 1 billion segments.
     */
    private static final int MAX_SEGMENTS = (1 << 30);

    private static final double UNDEFINED_DOUBLE_CONFIG = Double.NaN;
    private static final ConcurrentHashMap<File, Void> FILE_LOCKING_CONTROL = new ConcurrentHashMap<>(128);
    private static final ChronicleHashCorruption.Listener DEFAULT_CHRONICLE_MAP_CORRUPTION_LISTENER =
            corruption -> {
                Jvm.error().on(ChronicleMapBuilder.class, corruption.message(), corruption.exception());
            };
    private static final int MAX_BOOTSTRAPPING_HEADER_SIZE = (int) MemoryUnit.KILOBYTES.toBytes(16);
    private static final boolean MAP_CREATION_DEBUG = Jvm.getBoolean("chronicle.map.creation.debug");
    private static final int FILE_LOCK_TIMEOUT = Jvm.getInteger("chronicle.map.file.lock.timeout.secs", 10);

    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;
    K averageKey;
    V averageValue;
    /**
     * Default timeout is 1 minute. Even loopback tests converge often in the course of seconds,
     * let alone WAN replication over many nodes might take tens of seconds.
     * <p>
     * TODO review
     */
    long cleanupTimeout = 1;
    TimeUnit cleanupTimeoutUnit = TimeUnit.MINUTES;
    boolean cleanupRemovedEntries = true;
    //////////////////////////////
    // Configuration fields
    DefaultValueProvider<K, V> defaultValueProvider = DefaultSpi.defaultValueProvider();
    byte replicationIdentifier = -1;
    MapMethods<K, V, ?> methods = DefaultSpi.mapMethods();
    MapEntryOperations<K, V, ?> entryOperations = mapEntryOperations();
    MapRemoteOperations<K, V, ?> remoteOperations = mapRemoteOperations();
    Runnable preShutdownAction;
    boolean skipCloseOnExitHook = false;
    private String name;
    // not final because of cloning
    @UsedViaReflection // Picked up via Jvm.getValue()
    private ChronicleMapBuilderPrivateAPI<K, V> privateAPI = new ChronicleMapBuilderPrivateAPI<>(this);
    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private long entriesPerSegment = -1L;
    private long actualChunksPerSegmentTier = -1L;
    private double averageKeySize = UNDEFINED_DOUBLE_CONFIG;
    private K sampleKey;
    private double averageValueSize = UNDEFINED_DOUBLE_CONFIG;
    private V sampleValue;
    private int actualChunkSize = 0;
    private int worstAlignment = -1;
    private int maxChunksPerEntry = -1;
    private int alignment = UNDEFINED_ALIGNMENT_CONFIG;
    private long entries = -1L;
    private double maxBloatFactor = 1.0;
    private boolean allowSegmentTiering = true;
    private double nonTieredSegmentsPercentile = 0.99999;
    private boolean aligned64BitMemoryOperationsAtomic = OS.is64Bit();
    private ChecksumEntries checksumEntries = ChecksumEntries.IF_PERSISTED;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean replicated;
    private boolean persisted;
    private String replicatedMapClassName = ReplicatedChronicleMap.class.getName();
    private boolean sparseFile = Jvm.getBoolean("chronicle.map.sparseFile");

    ChronicleMapBuilder(@NotNull final Class<K> keyClass, @NotNull final Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<>(keyClass);
        valueBuilder = new SerializationBuilder<>(valueClass);
    }

    /**
     * Returns a new {@code ChronicleMapBuilder} instance which is able to {@linkplain #create()
     * create} maps with the specified key and value classes.
     *
     * @param keyClass   class object used to infer key type and discover it's properties via
     *                   reflection
     * @param valueClass class object used to infer value type and discover it's properties via
     *                   reflection
     * @param <K>        key type of the maps, created by the returned builder
     * @param <V>        value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilder<K, V> of(@NotNull final Class<K> keyClass, @NotNull final Class<V> valueClass) {
        return new ChronicleMapBuilder<>(keyClass, valueClass);
    }

    /**
     * Returns a new {@code ChronicleMapBuilder} instance which is able to {@linkplain #create()
     * create} maps with the specified key and value classes. It makes some assumptions about the size of entries
     * and the marshallers used
     *
     * @param keyClass   class object used to infer key type and discover it's properties via
     *                   reflection
     * @param valueClass class object used to infer value type and discover it's properties via
     *                   reflection
     * @param <K>        key type of the maps, created by the returned builder
     * @param <V>        value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilder<K, V> simpleMapOf(@NotNull final Class<K> keyClass, @NotNull final Class<V> valueClass) {
        final ChronicleMapBuilder<K, V> builder = new ChronicleMapBuilder<>(keyClass, valueClass)
                .putReturnsNull(true)
                .removeReturnsNull(true);
        builder.name(toCamelCase(valueClass.getSimpleName()));
        if (!builder.isKeySizeKnown())
            builder.averageKeySize(128);
        if (!builder.isValueSizeKnown())
            builder.averageValueSize(512);
        if (BytesMarshallable.class.isAssignableFrom(valueClass)) {
            builder.valueMarshaller(new BytesMarshallableReaderWriter<>((Class) valueClass));
        } else if (Marshallable.class.isAssignableFrom(valueClass)) {
            //noinspection unchecked
            builder.averageValueSize(1024)
                    .valueMarshaller(
                            valueClass.isMemberClass() && Modifier.isFinal(valueClass.getModifiers())
                                    ? new MarshallableReaderWriter<>((Class) valueClass)
                                    : new TypedMarshallableReaderWriter<>((Class) valueClass));
        }
        return builder;
    }

    private static String toCamelCase(@NotNull final String name) {
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }

    private static void checkSegments(final long segments) {
        if (segments <= 0) {
            throw new IllegalArgumentException("segments should be positive, " + segments + " given");
        }
        if (segments > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Max segments is " + MAX_SEGMENTS + ", " + segments + " given");
        }
    }

    private static String pretty(final int value) {
        return value > 0 ? value + "" : "not configured";
    }

    private static String pretty(final Object obj) {
        return obj != null ? obj + "" : "not configured";
    }

    private static void checkSizeIsStaticallyKnown(@NotNull final SerializationBuilder builder, @NotNull final String role) {
        if (builder.sizeIsStaticallyKnown) {
            throw new IllegalStateException("Size of " + builder.tClass +
                    " instances is constant and statically known, shouldn't be specified via " +
                    "average" + role + "Size() or average" + role + "() methods");
        }
    }

    private static void checkAverageSize(final double averageSize, @NotNull final String role) {
        if (averageSize <= 0 || isNaN(averageSize) || Double.isInfinite(averageSize)) {
            throw new IllegalArgumentException("Average " + role + " size must be a positive, " + "finite number");
        }
    }

    private static double averageSizeStoringLength(@NotNull final SerializationBuilder builder, final double averageSize) {
        final SizeMarshaller sizeMarshaller = builder.sizeMarshaller();
        if (averageSize == round(averageSize))
            return sizeMarshaller.storingLength(round(averageSize));
        final long lower = roundDown(averageSize);
        final long upper = lower + 1;
        final int lowerStoringLength = sizeMarshaller.storingLength(lower);
        final int upperStoringLength = sizeMarshaller.storingLength(upper);
        if (lowerStoringLength == upperStoringLength)
            return lowerStoringLength;
        return lower * (upper - averageSize) + upper * (averageSize - lower);
    }

    static int greatestCommonDivisor(final int a, final int b) {
        if (b == 0) return a;
        return greatestCommonDivisor(b, a % b);
    }

    private static int maxDefaultChunksPerAverageEntry(final boolean replicated, int avgEntrySize) {
        // When replicated, having 16 chunks (=> 8 bits in bitsets) per entry seems more wasteful
        // because when replicated we have bit sets per each remote node, not only allocation
        // bit set as when non-replicated
        if (avgEntrySize >= 2 * 64 * 64)
            return replicated ? 32 : 64;
        if (avgEntrySize >= 2 * 32 * 32)
            return replicated ? 16 : 32;
        if (avgEntrySize >= 2 * 16 * 16)
            return replicated ? 8 : 16;
        return replicated ? 4 : 8;
    }

    private static int estimateSegmentsForEntries(final long size) {
        if (size >= 256 << 20)
            return 256;
        if (size >= 32 << 20)
            return 128;
        if (size >= 4 << 20)
            return 64;
        if (size >= 512 << 10)
            return 32;
        if (size >= 64 << 10)
            return 16;
        if (size >= 8 << 10)
            return 8;
        if (size >= 1 << 10)
            return 4;
        return 1;
    }

    private static long headerChecksum(@NotNull final ByteBuffer headerBuffer, final int headerSize) {
        return LongHashFunction.xx_r39().hashBytes(headerBuffer, SIZE_WORD_OFFSET, headerSize + 4);
    }

    private static void writeNotComplete(@NotNull final FileChannel fileChannel,
                                         @NotNull final ByteBuffer headerBuffer,
                                         final int headerSize) throws IOException {
        //noinspection PointlessBitwiseExpression
        headerBuffer.putInt(SIZE_WORD_OFFSET, NOT_COMPLETE | DATA | headerSize);
        headerBuffer.clear().position(SIZE_WORD_OFFSET).limit(SIZE_WORD_OFFSET + 4);
        writeFully(fileChannel, SIZE_WORD_OFFSET, headerBuffer);
    }

    /**
     * @return ByteBuffer, with self bootstrapping header in [position, limit) range
     */
    private static <K, V> ByteBuffer writeHeader(@NotNull final FileChannel fileChannel, @NotNull final VanillaChronicleMap<K, V, ?> map) throws IOException {
        final ByteBuffer headerBuffer = ByteBuffer.allocate(
                SELF_BOOTSTRAPPING_HEADER_OFFSET + MAX_BOOTSTRAPPING_HEADER_SIZE);
        headerBuffer.order(LITTLE_ENDIAN);

        final Bytes<ByteBuffer> headerBytes = Bytes.wrapForWrite(headerBuffer);
        headerBytes.writePosition(SELF_BOOTSTRAPPING_HEADER_OFFSET);
        final Wire wire = new TextWire(headerBytes);
        wire.getValueOut().typedMarshallable(map);

        final int headerLimit = (int) headerBytes.writePosition();
        final int headerSize = headerLimit - SELF_BOOTSTRAPPING_HEADER_OFFSET;
        // First set readiness bit to READY, to compute checksum correctly
        //noinspection PointlessBitwiseExpression
        headerBuffer.putInt(SIZE_WORD_OFFSET, READY | DATA | headerSize);

        final long checksum = headerChecksum(headerBuffer, headerSize);
        headerBuffer.putLong(HEADER_OFFSET, checksum);

        // Set readiness bit to NOT_COMPLETE, because the Chronicle Map instance is not actually
        // ready yet
        //noinspection PointlessBitwiseExpression
        headerBuffer.putInt(SIZE_WORD_OFFSET, NOT_COMPLETE | DATA | headerSize);

        // Write the size-prefixed blob to the file
        headerBuffer.position(0);
        headerBuffer.limit(headerLimit);
        writeFully(fileChannel, 0, headerBuffer);

        headerBuffer.position(SELF_BOOTSTRAPPING_HEADER_OFFSET);
        return headerBuffer;
    }

    private static void commitChronicleMapReady(@NotNull final VanillaChronicleHash map,
                                                @NotNull final RandomAccessFile raf,
                                                @NotNull final ByteBuffer headerBuffer,
                                                final int headerSize) throws IOException {
        final FileChannel fileChannel = raf.getChannel();
        // see https://higherfrequencytrading.atlassian.net/browse/HCOLL-396
        map.msync();

        //noinspection PointlessBitwiseExpression
        headerBuffer.putInt(SIZE_WORD_OFFSET, READY | DATA | headerSize);
        headerBuffer.clear().position(SIZE_WORD_OFFSET).limit(SIZE_WORD_OFFSET + 4);
        writeFully(fileChannel, SIZE_WORD_OFFSET, headerBuffer);
    }

    /**
     * @return ByteBuffer, in [position, limit) range the self bootstrapping header is read
     */
    private static ByteBuffer readSelfBootstrappingHeader(@NotNull final File file,
                                                          @NotNull final RandomAccessFile raf,
                                                          final int headerSize,
                                                          final boolean recover,
                                                          @Nullable final ChronicleHashCorruption.Listener corruptionListener,
                                                          @Nullable final ChronicleHashCorruptionImpl corruption) throws IOException {
        if (raf.length() < headerSize + SELF_BOOTSTRAPPING_HEADER_OFFSET) {
            throw throwRecoveryOrReturnIOException(file,
                    "The file is shorter than the header size: " + headerSize +
                            ", file size: " + raf.length(), recover);
        }
        final FileChannel fileChannel = raf.getChannel();
        final ByteBuffer headerBuffer = ByteBuffer.allocate(SELF_BOOTSTRAPPING_HEADER_OFFSET + headerSize);
        headerBuffer.order(LITTLE_ENDIAN);
        readFully(fileChannel, 0, headerBuffer);
        if (headerBuffer.remaining() > 0) {
            throw throwRecoveryOrReturnIOException(file, "Unable to read the header fully, " +
                    headerBuffer.remaining() + " is remaining to read, likely the file was " +
                    "truncated", recover);
        }
        final int sizeWord = headerBuffer.getInt(SIZE_WORD_OFFSET);
        if (!SizePrefixedBlob.isReady(sizeWord)) {
            if (recover) {
                report(corruptionListener, corruption, -1, () ->
                        format("file={}: size-prefixed blob readiness bit is set to NOT_COMPLETE",
                                file)
                );
                // the bit will be overwritten to READY in the end of recovery procedure, so nothing
                // to fix right here
            } else {
                throw new IOException("file=" + file + ": sizeWord is not ready: " + sizeWord);
            }
        }
        headerBuffer.position(SELF_BOOTSTRAPPING_HEADER_OFFSET);
        return headerBuffer;
    }

    private static boolean checkSumSelfBootstrappingHeader(@NotNull final ByteBuffer headerBuffer, final int headerSize) {
        final long checkSum = headerChecksum(headerBuffer, headerSize);
        final long storedChecksum = headerBuffer.getLong(HEADER_OFFSET);
        return storedChecksum == checkSum;
    }

    private static boolean isDefined(final double config) {
        return !isNaN(config);
    }

    private static long toLong(final double v) {
        long l = round(v);
        if (l != v)
            throw new IllegalArgumentException("Integer argument expected, given " + v);
        return l;
    }

    private static long roundUp(final double v) {
        return round(Math.ceil(v));
    }

    private static long roundDown(final double v) {
        return (long) v;
    }

    private boolean isKeySizeKnown() {
        return keyBuilder.sizeIsStaticallyKnown;
    }

    private boolean isValueSizeKnown() {
        return valueBuilder.sizeIsStaticallyKnown;
    }

    @Override
    public ChronicleMapBuilder<K, V> clone() {
        try {
            @SuppressWarnings("unchecked") final ChronicleMapBuilder<K, V> result = (ChronicleMapBuilder<K, V>) super.clone();
            result.keyBuilder = keyBuilder.clone();
            result.valueBuilder = valueBuilder.clone();
            result.privateAPI = new ChronicleMapBuilderPrivateAPI<>(result);
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * {@inheritDoc}
     * <a href="#jvm-configurations">Read more about JVM-level configurations</a>.
     */
    @Override
    public ChronicleMapBuilder<K, V> name(@NotNull final String name) {
        this.name = name;
        return this;
    }

    String name() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>Example: if keys in your map(s) are English words in {@link String} form, average English
     * word length is 5.1, configure average key size of 6: <pre>{@code
     * ChronicleMap<String, LongValue> wordFrequencies = ChronicleMapBuilder
     *     .of(String.class, LongValue.class)
     *     .entries(50000)
     *     .averageKeySize(6)
     *     .create();}</pre>
     * (Note that 6 is chosen as average key size in bytes despite strings in Java are UTF-16
     * encoded (and each character takes 2 bytes on-heap), because default off-heap {@link String}
     * encoding is UTF-8 in {@code ChronicleMap}.)
     *
     * @param averageKeySize the average size of the key
     * @throws IllegalStateException    {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @see #averageKey(Object)
     * @see #constantKeySizeBySample(Object)
     * @see #averageValueSize(double)
     * @see #actualChunkSize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> averageKeySize(final double averageKeySize) {
        checkSizeIsStaticallyKnown(keyBuilder, "Key");
        checkAverageSize(averageKeySize, "key");
        this.averageKeySize = averageKeySize;
        averageKey = null;
        sampleKey = null;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param averageKey the average (by footprint in serialized form) key, is going to be put
     *                   into the hash containers, created by this builder
     * @throws NullPointerException {@inheritDoc}
     * @see #averageKeySize(double)
     * @see #constantKeySizeBySample(Object)
     * @see #averageValue(Object)
     * @see #actualChunkSize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> averageKey(@NotNull final K averageKey) {
        checkSizeIsStaticallyKnown(keyBuilder, "Key");
        this.averageKey = averageKey;
        sampleKey = null;
        averageKeySize = UNDEFINED_DOUBLE_CONFIG;
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>For example, if your keys are Git commit hashes:<pre>{@code
     * Map<byte[], String> gitCommitMessagesByHash =
     *     ChronicleMapBuilder.of(byte[].class, String.class)
     *     .constantKeySizeBySample(new byte[20])
     *     .create();}</pre>
     *
     * @see #averageKeySize(double)
     * @see #averageKey(Object)
     * @see #constantValueSizeBySample(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> constantKeySizeBySample(@NotNull final K sampleKey) {
        this.sampleKey = sampleKey;
        averageKey = null;
        averageKeySize = UNDEFINED_DOUBLE_CONFIG;
        return this;
    }

    private double averageKeySize() {
        if (!isDefined(averageKeySize))
            throw new AssertionError();
        return averageKeySize;
    }

    /**
     * Configures the average number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. However, in many cases {@link #averageValue(Object)} might be easier
     * to use and more reliable. If value size is always the same, call {@link
     * #constantValueSizeBySample(Object)} method instead of this one.
     * <p>
     * <p>{@code ChronicleHashBuilder} implementation heuristically chooses {@linkplain
     * #actualChunkSize(int) the actual chunk size} based on this configuration and the key size,
     * that, however, might result to quite high internal fragmentation, i. e. losses because only
     * integral number of chunks could be allocated for the entry. If you want to avoid this, you
     * should manually configure the actual chunk size in addition to this average value size
     * configuration, which is anyway needed.
     * <p>
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and shouldn't be specified by user.
     * <p>
     * <p>Calling this method clears any previous {@link #constantValueSizeBySample(Object)} and
     * {@link #averageValue(Object)} configurations.
     *
     * @param averageValueSize number of bytes, taken by serialized form of values
     * @return this builder back
     * @throws IllegalStateException    if value size is known statically and shouldn't be
     *                                  configured by user
     * @throws IllegalArgumentException if the given {@code averageValueSize} is non-positive
     * @see #averageValue(Object)
     * @see #constantValueSizeBySample(Object)
     * @see #averageKeySize(double)
     * @see #actualChunkSize(int)
     */
    public ChronicleMapBuilder<K, V> averageValueSize(final double averageValueSize) {
        checkSizeIsStaticallyKnown(valueBuilder, "Value");
        checkAverageSize(averageValueSize, "value");
        this.averageValueSize = averageValueSize;
        averageValue = null;
        sampleValue = null;
        return this;
    }

    /**
     * Configures the average number of bytes, taken by serialized form of values, put into maps,
     * created by this builder, by serializing the given {@code averageValue} using the configured
     * {@link #valueMarshallers(SizedReader, SizedWriter) value marshallers}. In some cases, {@link
     * #averageValueSize(double)} might be easier to use, than constructing the "average value".
     * If value size is always the same, call {@link #constantValueSizeBySample(Object)} method
     * instead of this one.
     * <p>
     * <p>{@code ChronicleHashBuilder} implementation heuristically chooses {@linkplain
     * #actualChunkSize(int) the actual chunk size} based on this configuration and the key size,
     * that, however, might result to quite high internal fragmentation, i. e. losses because only
     * integral number of chunks could be allocated for the entry. If you want to avoid this, you
     * should manually configure the actual chunk size in addition to this average value size
     * configuration, which is anyway needed.
     * <p>
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and shouldn't be specified by user.
     * <p>
     * <p>Calling this method clears any previous {@link #constantValueSizeBySample(Object)}
     * and {@link #averageValueSize(double)} configurations.
     *
     * @param averageValue the average (by footprint in serialized form) value, is going to be put
     *                     into the maps, created by this builder
     * @return this builder back
     * @throws NullPointerException if the given {@code averageValue} is {@code null}
     * @see #averageValueSize(double)
     * @see #constantValueSizeBySample(Object)
     * @see #averageKey(Object)
     * @see #actualChunkSize(int)
     */
    public ChronicleMapBuilder<K, V> averageValue(@NotNull final V averageValue) {
        final Class<?> valueClass = averageValue.getClass();
        if (BytesMarshallable.class.isAssignableFrom(valueClass) &&
                (valueBuilder.tClass.isInterface() && valueBuilder.tClass != Marshallable.class)) {
            if (Serializable.class.isAssignableFrom(valueClass))
                Jvm.warn().on(getClass(), "BytesMarshallable " + valueClass + " will be serialized as Serializable as the value class is an interface");
            else
                throw new IllegalArgumentException("Using BytesMarshallable and an interface value type not supported");
        }
        checkSizeIsStaticallyKnown(valueBuilder, "Value");
        this.averageValue = averageValue;
        sampleValue = null;
        averageValueSize = UNDEFINED_DOUBLE_CONFIG;
        return this;
    }

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. This is done by providing the {@code sampleValue}, all values should
     * take the same number of bytes in serialized form, as this sample object.
     * <p>
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and this method shouldn't be called.
     * <p>
     * <p>If value size varies, method {@link #averageValue(Object)} or {@link
     * #averageValueSize(double)} should be called instead of this one.
     * <p>
     * <p>Calling this method clears any previous {@link #averageValue(Object)} and
     * {@link #averageValueSize(double)} configurations.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #averageValueSize(double)
     * @see #averageValue(Object)
     * @see #constantKeySizeBySample(Object)
     */
    public ChronicleMapBuilder<K, V> constantValueSizeBySample(@NotNull final V sampleValue) {
        this.sampleValue = sampleValue;
        averageValue = null;
        averageValueSize = UNDEFINED_DOUBLE_CONFIG;
        return this;
    }

    double averageValueSize() {
        if (!isDefined(averageValueSize))
            throw new AssertionError();
        return averageValueSize;
    }

    private <E> double averageKeyOrValueSize(final double configuredSize,
                                             @NotNull final SerializationBuilder<E> builder,
                                             final E average) {
        if (isDefined(configuredSize))
            return configuredSize;
        if (builder.constantSizeMarshaller())
            return builder.constantSize();
        if (average != null) {
            return builder.serializationSize(average);
        }
        return Double.NaN;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException is sizes of both keys and values of maps created by this
     *                               builder are constant, hence chunk size shouldn't be configured by user
     * @see #entryAndValueOffsetAlignment(int)
     * @see #entries(long)
     * @see #maxChunksPerEntry(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> actualChunkSize(final int actualChunkSize) {
        if (constantlySizedEntries()) {
            throw new IllegalStateException("Sizes of key type: " + keyBuilder.tClass + " and " +
                    "value type: " + valueBuilder.tClass + " are both constant, " +
                    "so chunk size shouldn't be specified manually");
        }
        if (actualChunkSize <= 0)
            throw new IllegalArgumentException("Chunk size must be positive");
        if (alignment > 0 && actualChunkSize % alignment != 0)
            throw new IllegalArgumentException("The chunk size (" + actualChunkSize + ") must be a multiple of the alignment (" + alignment + ")");
        this.actualChunkSize = actualChunkSize;
        return this;
    }

    SerializationBuilder<K> keyBuilder() {
        return keyBuilder;
    }

    private EntrySizeInfo entrySizeInfo() {
        double size = 0;
        final double keySize = averageKeySize();
        size += averageSizeStoringLength(keyBuilder, keySize);
        size += keySize;
        if (replicated)
            size += ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;
        if (checksumEntries())
            size += ChecksumStrategy.CHECKSUM_STORED_BYTES;
        double valueSize = averageValueSize();
        size += averageSizeStoringLength(valueBuilder, valueSize);
        int alignment = valueAlignment();
        size = alignAddr((long) Math.ceil(size), alignment); // so the value starts aligned
        int worstAlignment;
        if (worstAlignmentComputationRequiresValueSize(alignment)) {
            final long constantSizeBeforeAlignment = toLong(size);
            if (constantlySizedValues()) {
                // see tierEntrySpaceInnerOffset()
                long totalDataSize = constantSizeBeforeAlignment + constantValueSize();
                worstAlignment = (int) (alignAddr(totalDataSize, alignment) - totalDataSize);
            } else {
                determineAlignment:
                if (actualChunkSize > 0) {
                    worstAlignment = worstAlignmentAssumingChunkSize(constantSizeBeforeAlignment,
                            actualChunkSize);
                } else {
                    int chunkSize = 8;
                    worstAlignment = worstAlignmentAssumingChunkSize(
                            constantSizeBeforeAlignment, chunkSize);
                    if (size + worstAlignment + valueSize >=
                            maxDefaultChunksPerAverageEntry(replicated, (int) size) * chunkSize) {
                        break determineAlignment;
                    }
                    chunkSize = 4;
                    worstAlignment = worstAlignmentAssumingChunkSize(
                            constantSizeBeforeAlignment, chunkSize);
                }
            }
        } else {
            // assume worst case, we always lose most possible bytes for alignment
            worstAlignment = worstAlignmentWithoutValueSize(alignment);
        }
        size += worstAlignment;
        size += valueSize;
        return new EntrySizeInfo(size, worstAlignment);
    }

    private boolean worstAlignmentComputationRequiresValueSize(final int alignment) {
        return alignment != NO_ALIGNMENT &&
                constantlySizedKeys() && valueBuilder.constantStoringLengthSizeMarshaller();
    }

    private int worstAlignmentWithoutValueSize(final int alignment) {
        return alignment - 1;
    }

    int segmentEntrySpaceInnerOffset() {
        // This is needed, if chunkSize = constant entry size is not aligned, for entry alignment
        // to be always the same, we should _misalign_ the first chunk.
        if (!constantlySizedEntries())
            return 0;
        return (int) (constantValueSize() % valueAlignment());
    }

    private long constantValueSize() {
        return valueBuilder.constantSize();
    }

    public boolean constantlySizedKeys() {
        return keyBuilder.constantSizeMarshaller() || sampleKey != null;
    }

    private int worstAlignmentAssumingChunkSize(final long constantSizeBeforeAlignment, final int chunkSize) {
        final int alignment = valueAlignment();
        final long firstAlignment = alignAddr(constantSizeBeforeAlignment, alignment) -
                constantSizeBeforeAlignment;
        final int gcdOfAlignmentAndChunkSize = greatestCommonDivisor(alignment, chunkSize);
        if (gcdOfAlignmentAndChunkSize == alignment)
            return (int) firstAlignment;
        // assume worst by now because we cannot predict alignment in VanillaCM.entrySize() method
        // before allocation
        long worstAlignment = firstAlignment;
        while (worstAlignment + gcdOfAlignmentAndChunkSize < alignment)
            worstAlignment += gcdOfAlignmentAndChunkSize;
        return (int) worstAlignment;
    }

    int worstAlignment() {
        if (worstAlignment >= 0)
            return worstAlignment;
        final int alignment = valueAlignment();
        if (!worstAlignmentComputationRequiresValueSize(alignment))
            return worstAlignment = worstAlignmentWithoutValueSize(alignment);
        return worstAlignment = entrySizeInfo().worstAlignment;
    }

    void worstAlignment(final int worstAlignment) {
        assert worstAlignment >= 0;
        this.worstAlignment = worstAlignment;
    }

    long chunkSize() {
        if (actualChunkSize > 0)
            return actualChunkSize;
        final double averageEntrySize = entrySizeInfo().averageEntrySize;
        if (constantlySizedEntries())
            return toLong(averageEntrySize);
        final int maxChunkSize = 1 << 30;
        for (int chunkSize = 4; chunkSize <= maxChunkSize; chunkSize *= 2L) {
            if ((long) maxDefaultChunksPerAverageEntry(replicated, (int) averageEntrySize) * chunkSize > averageEntrySize)
                return chunkSize;
        }
        return maxChunkSize;
    }

    boolean constantlySizedEntries() {
        return constantlySizedKeys() && constantlySizedValues();
    }

    double averageChunksPerEntry() {
        if (constantlySizedEntries())
            return 1.0;
        final long chunkSize = chunkSize();
        // assuming we always has worst internal fragmentation. This affects total segment
        // entry space which is allocated lazily on Linux (main target platform)
        // so we can afford this
        return (entrySizeInfo().averageEntrySize + chunkSize - 1) / chunkSize;
    }

    @Override
    public ChronicleMapBuilder<K, V> maxChunksPerEntry(final int maxChunksPerEntry) {
        if (maxChunksPerEntry < 1)
            throw new IllegalArgumentException("maxChunksPerEntry should be >= 1, " +
                    maxChunksPerEntry + " given");
        if (constantlySizedEntries()) {
            throw new IllegalStateException("Sizes of key type: " + keyBuilder.tClass + " and " +
                    "value type: " + valueBuilder.tClass + " are both constant, " +
                    "so maxChunksPerEntry shouldn't be specified manually");
        }
        this.maxChunksPerEntry = maxChunksPerEntry;
        return this;
    }

    int maxChunksPerEntry() {
        if (constantlySizedEntries())
            return 1;
        final long actualChunksPerSegmentTier = actualChunksPerSegmentTier();
        int result = (int) Math.min(actualChunksPerSegmentTier, (long) Integer.MAX_VALUE);
        if (this.maxChunksPerEntry > 0)
            result = Math.min(this.maxChunksPerEntry, result);
        return result;
    }

    public boolean constantlySizedValues() {
        return valueBuilder.constantSizeMarshaller() || sampleValue != null;
    }

    /**
     * Configures alignment of address in memory of entries and independently of address in memory
     * of values within entries ((i. e. final addresses in native memory are multiples of the given
     * alignment) for ChronicleMaps, created by this builder.
     * <p>
     * <p>Useful when values of the map are updated intensively, particularly fields with volatile
     * access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     * <p>
     * <p>If values couldn't reference off-heap memory (i. e. it is not {@link Byteable} or a value
     * interface), alignment configuration makes no sense.
     * <p>
     * <p>Default is {@link ValueModel#recommendedOffsetAlignment()} if the value type is a value
     * interface, otherwise 1 (that is effectively no alignment) or chosen heuristically (configure
     * explicitly for being sure and to compare performance in your case).
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this builder back
     * @throws IllegalStateException if values of maps, created by this builder, couldn't reference
     *                               off-heap memory
     */
    public ChronicleMapBuilder<K, V> entryAndValueOffsetAlignment(final int alignment) {
        if (alignment <= 0) {
            throw new IllegalArgumentException("Alignment should be positive integer, " + alignment + " given");
        }
        if (!isPowerOf2(alignment)) {
            throw new IllegalArgumentException("Alignment should be a power of 2, " + alignment + " given");
        }
        if (Jvm.isArm() && alignment < 8)
            return this;
        validateAlignment(actualChunkSize, actualChunkSize, alignment);

        this.alignment = alignment;
        return this;
    }

    public void validateAlignment(final int ifSet,
                                  final int actualChunkSize,
                                  final int alignment) {
        if (ifSet > 0 && actualChunkSize % alignment != 0)
            throw new IllegalArgumentException("The chunk size (" + actualChunkSize + ") must be a multiple of the alignment (" + alignment + ")");
    }

    int valueAlignment() {
        if (alignment != UNDEFINED_ALIGNMENT_CONFIG)
            return alignment;
        try {
            if (Values.isValueInterfaceOrImplClass(valueBuilder.tClass)) {
                final int alignment = ValueModel.acquire(valueBuilder.tClass).recommendedOffsetAlignment();
                validateAlignment(alignment, actualChunkSize, alignment);
                return alignment;
            } else {
                return NO_ALIGNMENT;
            }
        } catch (Exception e) {
            return NO_ALIGNMENT;
        }
    }

    @Override
    public ChronicleMapBuilder<K, V> entries(final long entries) {
        if (entries <= 0L)
            throw new IllegalArgumentException("Entries should be positive, " + entries + " given");
        this.entries = entries;
        return this;
    }

    long entries() {
        if (entries < 0) {
            throw new IllegalStateException("If in-memory Chronicle Map is created or persisted\n" +
                    "to a file for the first time (i. e. not accessing an existing file),\n" +
                    "ChronicleMapBuilder.entries() must be configured.\n" +
                    "See Chronicle Map 3 tutorial and javadocs for more information");
        }
        return entries;
    }

    @Override
    public ChronicleMapBuilder<K, V> entriesPerSegment(final long entriesPerSegment) {
        if (entriesPerSegment <= 0L)
            throw new IllegalArgumentException("Entries per segment should be positive, " + entriesPerSegment + " given");
        this.entriesPerSegment = entriesPerSegment;
        return this;
    }

    long entriesPerSegment() {
        long entriesPerSegment;
        if (this.entriesPerSegment > 0L) {
            entriesPerSegment = this.entriesPerSegment;
        } else {
            final int actualSegments = actualSegments();
            final double averageEntriesPerSegment = entries() * 1.0 / actualSegments;
            if (actualSegments > 1) {
                entriesPerSegment = PoissonDistribution.inverseCumulativeProbability(
                        averageEntriesPerSegment, nonTieredSegmentsPercentile);
            } else {
                // if there is only 1 segment, there is no source of variance in segments filling
                entriesPerSegment = roundUp(averageEntriesPerSegment);
            }
            if (sparseFile && maxBloatFactor > 1.0) {
                final double averageEntriesPerSegment2 = entries() * maxBloatFactor / actualSegments;
                entriesPerSegment = Math.max(roundUp(averageEntriesPerSegment2), entriesPerSegment);
            }
        }
        final boolean actualChunksDefined = actualChunksPerSegmentTier > 0;
        if (!actualChunksDefined) {
            final double averageChunksPerEntry = averageChunksPerEntry();
            if (entriesPerSegment * averageChunksPerEntry >
                    MAX_TIER_CHUNKS)
                throw new IllegalStateException("Max chunks per segment tier is " +
                        MAX_TIER_CHUNKS + " configured entries() and actualSegments() so that " +
                        "there should be " + entriesPerSegment + " entries per segment tier, " +
                        "while average chunks per entry is " + averageChunksPerEntry);
        }
        if (entriesPerSegment > MAX_TIER_ENTRIES)
            throw new IllegalStateException("shouldn't be more than " +
                    MAX_TIER_ENTRIES + " entries per segment");
        return entriesPerSegment;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualChunksPerSegmentTier(final long actualChunksPerSegmentTier) {
        if (actualChunksPerSegmentTier <= 0 || actualChunksPerSegmentTier > MAX_TIER_CHUNKS)
            throw new IllegalArgumentException("Actual chunks per segment tier should be in [1, " +
                    MAX_TIER_CHUNKS + "], range, " + actualChunksPerSegmentTier + " given");

        this.actualChunksPerSegmentTier = actualChunksPerSegmentTier;
        return this;
    }

    private void checkActualChunksPerSegmentTierIsConfiguredOnlyIfOtherLowLevelConfigsAreManual() {
        if (actualChunksPerSegmentTier > 0) {
            if (entriesPerSegment <= 0 || (actualChunkSize <= 0 && !constantlySizedEntries()) ||
                    actualSegments <= 0)
                throw new IllegalStateException("Actual chunks per segment tier could be " +
                        "configured only if other three low level configs are manual: " +
                        "entriesPerSegment(), actualSegments() and actualChunkSize(), unless " +
                        "both keys and value sizes are constant");
        }
    }

    private void checkActualChunksPerSegmentGreaterOrEqualToEntries() {
        if (actualChunksPerSegmentTier > 0 && entriesPerSegment > 0 &&
                entriesPerSegment > actualChunksPerSegmentTier) {
            throw new IllegalStateException("Entries per segment couldn't be greater than " +
                    "actual chunks per segment tier. Entries: " + entriesPerSegment + ", " +
                    "chunks: " + actualChunksPerSegmentTier + " is configured");
        }
    }

    long actualChunksPerSegmentTier() {
        if (actualChunksPerSegmentTier > 0)
            return actualChunksPerSegmentTier;
        return chunksPerSegmentTier(entriesPerSegment());
    }

    private long chunksPerSegmentTier(final long entriesPerSegment) {
        return roundUp(entriesPerSegment * averageChunksPerEntry());
    }

    @Override
    public ChronicleMapBuilder<K, V> minSegments(final int minSegments) {
        checkSegments(minSegments);
        this.minSegments = minSegments;
        return this;
    }

    int minSegments() {
        return Math.max(estimateSegments(), minSegments);
    }

    private int estimateSegments() {
        return (int) Math.min(nextPower2(entries() / 32, 1), estimateSegmentsBasedOnSize());
    }

    //TODO review because this heuristic doesn't seem to perform well
    private int estimateSegmentsBasedOnSize() {
        // the idea is that if values are huge, operations on them (and simply ser/deser)
        // could take long time, so we want more segment to minimize probablity that
        // two or more concurrent write ops will go to the same segment, and then all but one of
        // these threads will wait for long time.
        int segmentsForEntries = estimateSegmentsForEntries(entries());
        double averageValueSize = averageValueSize();
        return averageValueSize >= 1000000
                ? segmentsForEntries * 16
                : averageValueSize >= 100000
                ? segmentsForEntries * 8
                : averageValueSize >= 10000
                ? segmentsForEntries * 4
                : averageValueSize >= 1000
                ? segmentsForEntries * 2
                : segmentsForEntries;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualSegments(final int actualSegments) {
        checkSegments(actualSegments);
        this.actualSegments = actualSegments;
        return this;
    }

    int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        if (entriesPerSegment > 0) {
            return (int) segmentsGivenEntriesPerSegmentFixed(entriesPerSegment);
        }
        // Try to fit 4 bytes per hash lookup slot, then 8. Trying to apply small slot
        // size (=> segment size, because slot size depends on segment size) not only because
        // they take less memory per entry (if entries are of KBs or MBs, it doesn't matter), but
        // also because if segment size is small, slot and free list are likely to lie on a single
        // memory page, reducing number of memory pages to update, if Chronicle Map is persisted.
        // Actually small segments are all ways better: many segments => better parallelism, lesser
        // pauses for per-key operations, if parallel/background operation blocks the segment for
        // the whole time while it operates on it (like iteration, probably replication background
        // thread will require some level of full segment lock, however currently if doesn't, in
        // future durability background thread could update slot states), because smaller segments
        // contain less entries/slots and are processed faster.
        //
        // The only problem with small segments is that due to probability theory, if there are
        // a lot of segments each of little number of entries, difference between most filled
        // and least filled segment in the Chronicle Map grows. (Number of entries in a segment is
        // Poisson-distributed with mean = average number of entries per segment.) It is meaningful,
        // because segment tiering is exceptional mechanism, only very few segments should be
        // tiered, if any, normally. So, we are required to allocate unnecessarily many entries per
        // each segment. To compensate this at least on linux, don't accept segment sizes that with
        // the given entry sizes, lead to too small total segment sizes in native memory pages,
        // see comment in tryHashLookupSlotSize()
        final long segments = tryHashLookupSlotSize(4);
        if (segments > 0)
            return (int) segments;
        final int maxHashLookupEntrySize = aligned64BitMemoryOperationsAtomic() ? 8 : 4;
        final long maxEntriesPerSegment = findMaxEntriesPerSegmentToFitHashLookupSlotSize(maxHashLookupEntrySize);
        final long maxSegments = trySegments(maxEntriesPerSegment, MAX_SEGMENTS);
        if (maxSegments > 0L)
            return (int) maxSegments;
        throw new IllegalStateException("Max segments is " + MAX_SEGMENTS + ", configured so much" +
                " entries (" + entries() + ") or average chunks per entry is too high (" +
                averageChunksPerEntry() + ") that builder automatically decided to use " +
                (-maxSegments) + " segments");
    }

    private long tryHashLookupSlotSize(int hashLookupSlotSize) {
        final long entriesPerSegment = findMaxEntriesPerSegmentToFitHashLookupSlotSize(hashLookupSlotSize);
        final long entrySpaceSize = roundUp(entriesPerSegment * entrySizeInfo().averageEntrySize);
        // Not to lose too much on linux because of "poor distribution" entry over-allocation.
        // This condition should likely filter cases when we target very small hash lookup
        // size + entry size is small.
        // * 5 => segment will lose not more than 20% of memory, 10% on average
        if (entrySpaceSize < OS.pageSize() * 5L)
            return -1;
        return trySegments(entriesPerSegment, MAX_SEGMENTS);
    }

    private long findMaxEntriesPerSegmentToFitHashLookupSlotSize(int targetHashLookupSlotSize) {
        long entriesPerSegment = 1L << 62;
        long step = entriesPerSegment / 2L;
        while (step > 0L) {
            if (hashLookupSlotBytes(entriesPerSegment) > targetHashLookupSlotSize)
                entriesPerSegment -= step;
            step /= 2L;
        }
        return entriesPerSegment - 1L;
    }

    private int hashLookupSlotBytes(final long entriesPerSegment) {
        final int valueBits = valueBits(chunksPerSegmentTier(entriesPerSegment));
        final int keyBits = keyBits(entriesPerSegment, valueBits);
        return entrySize(keyBits, valueBits);
    }

    private long trySegments(final long entriesPerSegment, final int maxSegments) {
        long segments = segmentsGivenEntriesPerSegmentFixed(entriesPerSegment);
        segments = nextPower2(Math.max(segments, minSegments()), 1L);
        return segments <= maxSegments ? segments : -segments;
    }

    private long segmentsGivenEntriesPerSegmentFixed(final long entriesPerSegment) {
        final double precision = 1.0 / averageChunksPerEntry();
        final long entriesPerSegmentShouldBe =
                roundDown(PoissonDistribution.meanByCumulativeProbabilityAndValue(
                        nonTieredSegmentsPercentile, entriesPerSegment, precision));
        long segments = divideRoundUp(entries(), entriesPerSegmentShouldBe);
        checkSegments(segments);
        if (minSegments > 0)
            segments = Math.max(minSegments, segments);
        return segments;
    }

    long tierHashLookupCapacity() {
        final long entriesPerSegment = entriesPerSegment();
        long capacity = CompactOffHeapLinearHashTable.capacityFor(entriesPerSegment);
        if (actualSegments() > 1) {
            // if there is only 1 segment, there is no source of variance in segments filling
            long maxEntriesPerTier = PoissonDistribution.inverseCumulativeProbability(
                    entriesPerSegment, nonTieredSegmentsPercentile);
            while (maxEntriesPerTier > MAX_LOAD_FACTOR * capacity) {
                capacity *= 2;
            }
        }
        return capacity;
    }

    int segmentHeaderSize() {
        final int segments = actualSegments();

        final long pageSize = OS.pageSize();
        if (segments * (64 * 3) < (2 * pageSize)) // i. e. <= 42 segments, if page size is 4K
            return 64 * 3; // cache line per header, plus one CL to the left, plus one to the right

        if (segments * (64 * 2) < (3 * pageSize)) // i. e. <= 96 segments, if page size is 4K
            return 64 * 2;

        // reduce false sharing unless we have a lot of segments.
        return segments <= 16 * 1024 ? 64 : 32;
    }

    /**
     * Configures if the maps created by this {@code ChronicleMapBuilder} should return {@code null}
     * instead of previous mapped values on {@link ChronicleMap#put(Object, Object)
     * ChornicleMap.put(key, value)} calls.
     * <p>
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality
     * which is rarely used but fairly cheap for simple in-process, on-heap implementations like
     * {@link HashMap}. But an off-heap collection has to create a new object and deserialize
     * the data from off-heap memory. A collection hiding remote queries over the network should
     * send the value back in addition to that. It's expensive for something you probably don't use.
     * <p>
     * <p>This is a <a href="#jvm-configurations">JVM-level configuration</a>.
     * <p>
     * <p>By default, {@code ChronicleMap} conforms the general {@code Map} contract and returns the
     * previous mapped value on {@code put()} calls.
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but
     *                       instead return {@code null}
     * @return this builder back
     * @see #removeReturnsNull(boolean)
     */
    public ChronicleMapBuilder<K, V> putReturnsNull(final boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Configures if the maps created by this {@code ChronicleMapBuilder} should return {@code null}
     * instead of the last mapped value on {@link ChronicleMap#remove(Object)
     * ChronicleMap.remove(key)} calls.
     * <p>
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is
     * rarely used but fairly cheap for simple in-process, on-heap implementations like {@link
     * HashMap}. But an off-heap collection has to create a new object and deserialize the data
     * from off-heap memory. A collection hiding remote queries over the network should send
     * the value back in addition to that. It's expensive for something you probably don't use.
     * <p>
     * <p>This is a <a href="#jvm-configurations">JVM-level configuration</a>.
     * <p>
     * <p>By default, {@code ChronicleMap} conforms the general {@code Map} contract and returns the
     * mapped value on {@code remove()} calls.
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry
     *                          but instead return {@code null}
     * @return this builder back
     * @see #putReturnsNull(boolean)
     */
    public ChronicleMapBuilder<K, V> removeReturnsNull(final boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Override
    public ChronicleMapBuilder<K, V> maxBloatFactor(final double maxBloatFactor) {
        if (isNaN(maxBloatFactor) || maxBloatFactor < 1.0 || maxBloatFactor > 1_000.0) {
            throw new IllegalArgumentException("maxBloatFactor should be in [1.0, 1_000.0] " +
                    "bounds, " + maxBloatFactor + " given");
        }
        this.maxBloatFactor = maxBloatFactor;
        return this;
    }

    public double maxBloatFactor() {
        return maxBloatFactor;
    }

    @Override
    public ChronicleMapBuilder<K, V> allowSegmentTiering(final boolean allowSegmentTiering) {
        this.allowSegmentTiering = allowSegmentTiering;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> nonTieredSegmentsPercentile(final double nonTieredSegmentsPercentile) {
        if (isNaN(nonTieredSegmentsPercentile) ||
                0.5 <= nonTieredSegmentsPercentile || nonTieredSegmentsPercentile >= 1.0) {
            throw new IllegalArgumentException("nonTieredSegmentsPercentile should be in (0.5, " +
                    "1.0) range, " + nonTieredSegmentsPercentile + " is given");
        }
        this.nonTieredSegmentsPercentile = nonTieredSegmentsPercentile;
        return this;
    }

    long maxExtraTiers() {
        if (!allowSegmentTiering)
            return 0;
        final int actualSegments = actualSegments();
        // maxBloatFactor is scale, so we do (- 1.0) to compute _extra_ tiers
        return round((maxBloatFactor - 1.0) * actualSegments)
                // but to mitigate slight misconfiguration, and uneven distribution of entries
                // between segments, add 1.0 x actualSegments
                + actualSegments;
    }

    @Override
    public String toString() {
        return "ChronicleMapBuilder{" +
                ", actualSegments=" + pretty(actualSegments) +
                ", minSegments=" + pretty(minSegments) +
                ", entriesPerSegment=" + pretty(entriesPerSegment) +
                ", actualChunksPerSegmentTier=" + pretty(actualChunksPerSegmentTier) +
                ", averageKeySize=" + pretty(averageKeySize) +
                ", sampleKeyForConstantSizeComputation=" + pretty(sampleKey) +
                ", averageValueSize=" + pretty(averageValueSize) +
                ", sampleValueForConstantSizeComputation=" + pretty(sampleValue) +
                ", actualChunkSize=" + pretty(actualChunkSize) +
                ", valueAlignment=" + valueAlignment() +
                ", entries=" + entries() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", sparseFile=" + sparseFile() +
                ", keyBuilder=" + keyBuilder +
                ", valueBuilder=" + valueBuilder +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object o) {
        return builderEquals(this, o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    ChronicleMapBuilder<K, V> removedEntryCleanupTimeout(final long removedEntryCleanupTimeout,
                                                         @NotNull final TimeUnit unit) {
        if (unit.toMillis(removedEntryCleanupTimeout) < 1) {
            throw new IllegalArgumentException("timeout should be >= 1 millisecond, " +
                    removedEntryCleanupTimeout + " " + unit + " is given");
        }
        cleanupTimeout = removedEntryCleanupTimeout;
        cleanupTimeoutUnit = unit;
        return this;
    }

    ChronicleMapBuilder<K, V> cleanupRemovedEntries(final boolean cleanupRemovedEntries) {
        this.cleanupRemovedEntries = cleanupRemovedEntries;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyReaderAndDataAccess(final SizedReader<K> keyReader, @NotNull final DataAccess<K> keyDataAccess) {
        keyBuilder.reader(keyReader);
        keyBuilder.dataAccess(keyDataAccess);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshallers(@NotNull final SizedReader<K> keyReader, @NotNull final SizedWriter<? super K> keyWriter) {
        keyBuilder.reader(keyReader);
        keyBuilder.writer(keyWriter);
        return this;
    }

    @Override
    public <M extends SizedReader<K> & SizedWriter<? super K>>
    ChronicleMapBuilder<K, V> keyMarshaller(@NotNull final M sizedMarshaller) {
        return keyMarshallers(sizedMarshaller, sizedMarshaller);
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshallers(@NotNull final BytesReader<K> keyReader, @NotNull final BytesWriter<? super K> keyWriter) {
        keyBuilder.reader(keyReader);
        keyBuilder.writer(keyWriter);
        return this;
    }

    @Override
    public <M extends BytesReader<K> & BytesWriter<? super K>>
    ChronicleMapBuilder<K, V> keyMarshaller(@NotNull final M marshaller) {
        return keyMarshallers(marshaller, marshaller);
    }

    @Override
    public ChronicleMapBuilder<K, V> keySizeMarshaller(@NotNull final SizeMarshaller keySizeMarshaller) {
        keyBuilder.sizeMarshaller(keySizeMarshaller);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> aligned64BitMemoryOperationsAtomic(final boolean aligned64BitMemoryOperationsAtomic) {
        this.aligned64BitMemoryOperationsAtomic = aligned64BitMemoryOperationsAtomic;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> checksumEntries(final boolean checksumEntries) {
        this.checksumEntries = checksumEntries ? ChecksumEntries.YES : ChecksumEntries.NO;
        return this;
    }

    boolean checksumEntries() {
        switch (checksumEntries) {
            case NO:
                return false;
            case YES:
                return true;
            case IF_PERSISTED:
                return persisted;
            default:
                throw new AssertionError();
        }
    }

    boolean aligned64BitMemoryOperationsAtomic() {
        return aligned64BitMemoryOperationsAtomic;
    }

    /**
     * Configures the {@code DataAccess} and {@code SizedReader} used to serialize and deserialize
     * values to and from off-heap memory in maps, created by this builder.
     *
     * @param valueReader     the new bytes &rarr; value object reader strategy
     * @param valueDataAccess the new strategy of accessing the values' bytes for writing
     * @return this builder back
     * @see #valueMarshallers(SizedReader, SizedWriter)
     * @see ChronicleHashBuilder#keyReaderAndDataAccess(SizedReader, DataAccess)
     */
    public ChronicleMapBuilder<K, V> valueReaderAndDataAccess(@NotNull final SizedReader<V> valueReader, @NotNull final DataAccess<V> valueDataAccess) {
        valueBuilder.reader(valueReader);
        valueBuilder.dataAccess(valueDataAccess);
        return this;
    }

    /**
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory in
     * maps, created by this builder.
     *
     * @param valueReader the new bytes &rarr; value object reader strategy
     * @param valueWriter the new value object &rarr; bytes writer strategy
     * @return this builder back
     * @see #valueReaderAndDataAccess(SizedReader, DataAccess)
     * @see #valueSizeMarshaller(SizeMarshaller)
     * @see ChronicleHashBuilder#keyMarshallers(SizedReader, SizedWriter)
     */
    public ChronicleMapBuilder<K, V> valueMarshallers(@NotNull final SizedReader<V> valueReader, @NotNull final SizedWriter<? super V> valueWriter) {
        valueBuilder.reader(valueReader);
        valueBuilder.writer(valueWriter);
        return this;
    }

    /**
     * Shortcut for {@link #valueMarshallers(SizedReader, SizedWriter)
     * valueMarshallers(sizedMarshaller, sizedMarshaller)}.
     */
    public <M extends SizedReader<V> & SizedWriter<? super V>> ChronicleMapBuilder<K, V> valueMarshaller(@NotNull final M sizedMarshaller) {
        return valueMarshallers(sizedMarshaller, sizedMarshaller);
    }

    /**
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory in
     * maps, created by this builder.
     *
     * @param valueReader the new bytes &rarr; value object reader strategy
     * @param valueWriter the new value object &rarr; bytes writer strategy
     * @return this builder back
     * @see #valueReaderAndDataAccess(SizedReader, DataAccess)
     * @see #valueSizeMarshaller(SizeMarshaller)
     * @see ChronicleHashBuilder#keyMarshallers(BytesReader, BytesWriter)
     */
    public ChronicleMapBuilder<K, V> valueMarshallers(@NotNull final BytesReader<V> valueReader, @NotNull final BytesWriter<? super V> valueWriter) {
        valueBuilder.reader(valueReader);
        valueBuilder.writer(valueWriter);
        return this;
    }

    /**
     * Shortcut for {@link #valueMarshallers(BytesReader, BytesWriter)
     * valueMarshallers(marshaller, marshaller)}.
     */
    public <M extends BytesReader<V> & BytesWriter<? super V>> ChronicleMapBuilder<K, V> valueMarshaller(@NotNull final M marshaller) {
        return valueMarshallers(marshaller, marshaller);
    }

    /**
     * Configures the marshaller used to serialize actual value sizes to off-heap memory in maps,
     * created by this builder.
     * <p>
     * <p>Default value size marshaller is so-called "stop bit encoding" marshalling, unless {@link
     * #constantValueSizeBySample(Object)} or the builder statically knows the value size is
     * constant -- special constant size marshalling is used by default in these cases.
     *
     * @param valueSizeMarshaller the new marshaller, used to serialize actual value sizes to
     *                            off-heap memory
     * @return this builder back
     * @see #keySizeMarshaller(SizeMarshaller)
     */
    public ChronicleMapBuilder<K, V> valueSizeMarshaller(@NotNull final SizeMarshaller valueSizeMarshaller) {
        valueBuilder.sizeMarshaller(valueSizeMarshaller);
        return this;
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#acquireUsing
     * acquireUsing()} calls, if the key is absent in the map, created by this builder.
     * <p>
     * <p>This is a <a href="#jvm-configurations">JVM-level configuration</a>.
     *
     * @param defaultValueProvider the strategy to obtain a default value by the absent key
     * @return this builder back
     */
    public ChronicleMapBuilder<K, V> defaultValueProvider(@NotNull final DefaultValueProvider<K, V> defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        return this;
    }

    public ChronicleMapBuilder<K, V> replication(final byte identifier) {
        if (identifier <= 0)
            throw new IllegalArgumentException("Identifier must be positive, " + identifier +
                    " given");
        this.replicationIdentifier = identifier;
        return this;
    }

    public ChronicleMapBuilder<K, V> replicatedMapClassName(final String replicatedMapClassName) {
        this.replicatedMapClassName = replicatedMapClassName;
        return this;
    }

    /**
     * Enable the use of sparse files if supported. This will double the extents however could reduce disk space used.
     *
     * @param sparseFile true, if allowed
     * @return this builder
     */
    public ChronicleMapBuilder<K, V> sparseFile(final boolean sparseFile) {
        this.sparseFile = sparseFile;
        return this;
    }

    public boolean sparseFile() {
        return sparseFile;
    }

    @Override
    public ChronicleMap<K, V> createPersistedTo(@NotNull final File file) throws IOException {
        // clone() to make this builder instance thread-safe, because createWithFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithFile(file, false, false, null);
    }

    @Override
    public ChronicleMap<K, V> createOrRecoverPersistedTo(@NotNull final File file) throws IOException {
        return createOrRecoverPersistedTo(file, true);
    }

    @Override
    public ChronicleMap<K, V> createOrRecoverPersistedTo(@NotNull final File file, final boolean sameLibraryVersion)
            throws IOException {
        return createOrRecoverPersistedTo(file, sameLibraryVersion, DEFAULT_CHRONICLE_MAP_CORRUPTION_LISTENER);
    }

    @Override
    public ChronicleMap<K, V> createOrRecoverPersistedTo(@NotNull final File file,
                                                         final boolean sameLibraryVersion,
                                                         @Nullable final ChronicleHashCorruption.Listener corruptionListener) throws IOException {
        if (file.exists()) {
            return recoverPersistedTo(file, sameLibraryVersion, corruptionListener);
        } else {
            return createPersistedTo(file);
        }
    }

    @Override
    public ChronicleMap<K, V> recoverPersistedTo(@NotNull final File file, final boolean sameBuilderConfigAndLibraryVersion) throws IOException {
        return recoverPersistedTo(file, sameBuilderConfigAndLibraryVersion,
                DEFAULT_CHRONICLE_MAP_CORRUPTION_LISTENER);
    }

    @Override
    public ChronicleMap<K, V> recoverPersistedTo(@NotNull final File file,
                                                 final boolean sameBuilderConfigAndLibraryVersion,
                                                 @Nullable final ChronicleHashCorruption.Listener corruptionListener) throws IOException {
        AnalyticsHolder.instance().sendEvent("recover");
        return clone().createWithFile(file, true, sameBuilderConfigAndLibraryVersion, corruptionListener);
    }

    @Override
    public ChronicleMapBuilder<K, V> setPreShutdownAction(@NotNull final Runnable preShutdownAction) {
        this.preShutdownAction = preShutdownAction;
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> skipCloseOnExitHook(final boolean skipCloseOnExitHook) {
        this.skipCloseOnExitHook = skipCloseOnExitHook;
        return this;
    }

    @Override
    public ChronicleMap<K, V> create() {
        // clone() to make this builder instance thread-safe, because createWithoutFile() method
        // computes some state based on configurations, but doesn't synchronize on configuration
        // changes.
        return clone().createWithoutFile();
    }

    /**
     * Inject your SPI code around basic {@code ChronicleMap}'s operations with entries:
     * removing entries, replacing entries' value and inserting new entries.
     * <p>
     * <p>This affects behaviour of ordinary map.put(), map.remove(), etc. calls, as well as removes
     * and replacing values <i>during iterations</i>, <i>remote map calls</i> and
     * <i>internal replication operations</i>.
     * <p>
     * <p>This is a <a href="#jvm-configurations">JVM-level configuration</a>.
     *
     * @return this builder back
     */
    public ChronicleMapBuilder<K, V> entryOperations(@NotNull final MapEntryOperations<K, V, ?> entryOperations) {
        this.entryOperations = entryOperations;
        return this;
    }

    /**
     * Inject your SPI around logic of all {@code ChronicleMap}'s operations with individual keys:
     * from {@link ChronicleMap#containsKey} to {@link ChronicleMap#acquireUsing} and
     * {@link ChronicleMap#merge}.
     * <p>
     * <p>This affects behaviour of ordinary map calls, as well as <i>remote calls</i>.
     * <p>
     * <p>This is a <a href="#jvm-configurations">JVM-level configuration</a>.
     *
     * @return this builder back
     */
    public ChronicleMapBuilder<K, V> mapMethods(@NotNull final MapMethods<K, V, ?> mapMethods) {
        this.methods = mapMethods;
        return this;
    }

    ChronicleMapBuilder<K, V> remoteOperations(@NotNull final MapRemoteOperations<K, V, ?> remoteOperations) {
        this.remoteOperations = remoteOperations;
        return this;
    }

    private ChronicleMap<K, V> createWithFile(@NotNull final File file,
                                              final boolean recover,
                                              final boolean overrideBuilderConfig,
                                              @Nullable final ChronicleHashCorruption.Listener corruptionListener) throws IOException {
        if (overrideBuilderConfig && !recover)
            throw new AssertionError("recover -> overrideBuilderConfig");
        replicated = replicationIdentifier != -1;
        persisted = true;

        // This line pre-initializes CachedCompiler - the first call may take time as Javac needs to scan the whole classpath
        Values.nativeClassFor(VanillaGlobalMutableState.class);

        // It's important to canonicalize the file, because CanonicalRandomAccessFiles.acquire()
        // relies on java.io.File equality, which doesn't account symlinks itself.
        final File canonicalFile = file.getCanonicalFile();
        if (!canonicalFile.exists()) {
            if (recover)
                throw new FileNotFoundException("file " + canonicalFile + " should exist for recovery");
            //noinspection ResultOfMethodCallIgnored
            canonicalFile.createNewFile();
        }
        final RandomAccessFile raf = CanonicalRandomAccessFiles.acquire(canonicalFile);
        final ChronicleHashResources resources = new PersistedChronicleHashResources(canonicalFile);
        try {
            VanillaChronicleMap<K, V, ?> result;
            if (raf.length() > 0) {
                result = openWithExistingFile(canonicalFile, raf, resources, recover, overrideBuilderConfig, corruptionListener);
            } else {
                // Atomic* allows lambda modification
                final AtomicReference<VanillaChronicleMap<K, V, ?>> map = new AtomicReference<>();
                final AtomicReference<ByteBuffer> headerBuffer = new AtomicReference<>();
                final AtomicBoolean newFile = new AtomicBoolean(false);
                final FileChannel fileChannel = raf.getChannel();

                TimingPauser pauser = Pauser.balanced();

                while (raf.length() == 0) {
                    final boolean locked = CanonicalRandomAccessFiles.tryRunExclusively(canonicalFile, fileChannel, () -> {
                        // Double-checked locking
                        if (raf.length() == 0) {
                            map.set(newMap());
                            headerBuffer.set(writeHeader(fileChannel, map.get()));
                            newFile.set(true);
                        }
                    });

                    if (locked)
                        break;
                    else {
                        try {
                            pauser.pause(FILE_LOCK_TIMEOUT, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            Jvm.warn().on(getClass(), "Failed to write header: can't acquire exclusive file " +
                                    "lock on empty file [" + canonicalFile + "] for " + FILE_LOCK_TIMEOUT + " seconds", e);

                            Jvm.rethrow(e);
                        }
                    }
                }

                if (newFile.get()) {
                    final int headerSize = headerBuffer.get().remaining();
                    result = createWithNewFile(map.get(), canonicalFile, raf, resources, headerBuffer.get(), headerSize);
                } else {
                    result = openWithExistingFile(canonicalFile, raf, resources, recover, overrideBuilderConfig, corruptionListener);
                }
            }
            prepareMapPublication(result);
            return result;
        } catch (Throwable throwable) {
            try {
                try {
                    resources.setChronicleHashIdentityString(
                            "ChronicleHash{name=" + name + ", file=" + canonicalFile + "}");
                } catch (Throwable t) {
                    throwable.addSuppressed(t);
                } finally {
                    resources.releaseManually();
                }
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw Throwables.propagateNotWrapping(throwable, IOException.class);
        }
    }

    private void prepareMapPublication(@NotNull final VanillaChronicleMap<K, V, ?> map) throws IOException {
        establishReplication(map);
        map.setResourcesName();
        map.registerCleaner();
        // Ensure safe publication of the ChronicleMap
        OS.memory().storeFence();
        map.addToOnExitHook();
    }

    /**
     * @return size of the self bootstrapping header
     */
    private int waitUntilReady(@NotNull final RandomAccessFile raf,
                               @NotNull final File file,
                               final boolean recover) throws IOException {
        final FileChannel fileChannel = raf.getChannel();

        final ByteBuffer sizeWordBuffer = ByteBuffer.allocate(4);
        sizeWordBuffer.order(LITTLE_ENDIAN);

        // 60 * 10, 100 ms wait = 1 minute total wait
        final int attempts = 60 * 10;
        int lastReadHeaderSize = -1;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (raf.length() >= SELF_BOOTSTRAPPING_HEADER_OFFSET) {

                sizeWordBuffer.clear();
                readFully(fileChannel, SIZE_WORD_OFFSET, sizeWordBuffer);
                if (sizeWordBuffer.remaining() == 0) {
                    int sizeWord = sizeWordBuffer.getInt(0);
                    lastReadHeaderSize = SizePrefixedBlob.extractSize(sizeWord);
                    if (SizePrefixedBlob.isReady(sizeWord))
                        return lastReadHeaderSize;
                }
                // The only possible reason why not 4 bytes are read, is that the file is
                // truncated between length() and read() calls, then continue to wait
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (recover) {
                    if (lastReadHeaderSize == -1) {
                        throw new ChronicleHashRecoveryFailedException(e);
                    } else {
                        return lastReadHeaderSize;
                    }
                } else {
                    throw new IOException(e);
                }
            }
        }
        if (recover) {
            if (lastReadHeaderSize == -1) {
                throw new ChronicleHashRecoveryFailedException(
                        "File header is not recoverable, file=" + file);
            } else {
                return lastReadHeaderSize;
            }
        } else {
            throw new IOException("Unable to wait until the file=" + file +
                    " is ready, likely the process which created the file crashed or hung " +
                    "for more than 1 minute");
        }
    }

    private VanillaChronicleMap<K, V, ?> createWithNewFile(@NotNull final VanillaChronicleMap<K, V, ?> map,
                                                           @NotNull final File canonicalFile,
                                                           @NotNull final RandomAccessFile raf,
                                                           @NotNull final ChronicleHashResources resources,
                                                           @NotNull final ByteBuffer headerBuffer,
                                                           final int headerSize) throws IOException {
        if (MAP_CREATION_DEBUG) {
            Jvm.warn().on(getClass(), "<map creation debug> File [canonizedMapDataFile=" + canonicalFile.getAbsolutePath() +
                    "] is missing or empty, creating Map from scratch");
        }

        map.initBeforeMapping(canonicalFile, raf, headerBuffer.limit(), false);
        map.createMappedStoreAndSegments(resources);
        CanonicalRandomAccessFiles.acquireSharedFileLock(canonicalFile, raf.getChannel());
        map.addCloseable(() -> CanonicalRandomAccessFiles.releaseSharedFileLock(canonicalFile));
        commitChronicleMapReady(map, raf, headerBuffer, headerSize);
        return map;
    }

    private VanillaChronicleMap<K, V, ?> openWithExistingFile(@NotNull final File file,
                                                              @NotNull final RandomAccessFile raf,
                                                              @NotNull final ChronicleHashResources resources,
                                                              final boolean recover,
                                                              final boolean overrideBuilderConfig,
                                                              @Nullable final ChronicleHashCorruption.Listener corruptionListener) throws IOException {

        if (recover) {
            Jvm.warn().on(ChronicleMapBuilder.class, "Recovery operation needs exclusive access to the ChronicleMap or else the result is unspecified including the risk of loosing and/or corrupting partial or all data.");
            Jvm.warn().on(ChronicleMapBuilder.class, "Do not use recovery as a standard way of opening a ChronicleMap.");
        }

        final ChronicleHashCorruptionImpl corruption = recover ? new ChronicleHashCorruptionImpl() : null;
        try {
            int headerSize = waitUntilReady(raf, file, recover);
            final FileChannel fileChannel = raf.getChannel();
            ByteBuffer headerBuffer = readSelfBootstrappingHeader(file, raf, headerSize, recover, corruptionListener, corruption);
            if (headerSize != headerBuffer.remaining())
                throw new AssertionError();
            final boolean headerCorrect = checkSumSelfBootstrappingHeader(headerBuffer, headerSize);

            if (MAP_CREATION_DEBUG) {
                Jvm.warn().on(getClass(), "<map creation debug> Using existing file [canonizedMapDataFile=" + file.getAbsolutePath() +
                        ", size=" + raf.length() + ", headerCorrect=" + headerCorrect + "] for map creation");
            }

            boolean headerWritten = false;
            if (!headerCorrect) {
                if (overrideBuilderConfig) {
                    final VanillaChronicleMap<K, V, ?> mapObjectForHeaderOverwrite = newMap();
                    headerBuffer = writeHeader(fileChannel, mapObjectForHeaderOverwrite);
                    headerSize = headerBuffer.remaining();
                    headerWritten = true;
                } else {
                    throw throwRecoveryOrReturnIOException(file,
                            "Self Bootstrapping Header checksum doesn't match the stored checksum",
                            recover);
                }
            }
            final Bytes<ByteBuffer> headerBytes = Bytes.wrapForRead(headerBuffer);
            headerBytes.readPosition(headerBuffer.position());
            headerBytes.readLimit(headerBuffer.limit());
            final Wire wire = new TextWire(headerBytes);

            if (MAP_CREATION_DEBUG) {
                Jvm.warn().on(getClass(), "<map creation debug> Read header from file [canonizedMapDataFile=" +
                        file.getAbsolutePath() + "]: " + wire);
            }

            final VanillaChronicleMap<K, V, ?> map = wire.getValueIn().typedMarshallable();
            map.initBeforeMapping(file, raf, headerBuffer.limit(), recover);

            final long dataStoreSize = map.globalMutableState().getDataStoreSize();
            long fileLength = raf.length();
            if (!recover && dataStoreSize > fileLength) {
                throw new IOException("The file " + file + " the map is serialized from " +
                        "has unexpected length " + fileLength + ", probably corrupted. " +
                        "Data store size is " + dataStoreSize);
            }
            map.initTransientsFromBuilder(this);
            if (!recover) {
                map.createMappedStoreAndSegments(resources);
                CanonicalRandomAccessFiles.acquireSharedFileLock(file, raf.getChannel());
            } else {
                if (!headerWritten)
                    writeNotComplete(fileChannel, headerBuffer, headerSize);
                try {
                    CanonicalRandomAccessFiles.acquireExclusiveFileLock(file, raf.getChannel());
                    map.recover(resources, corruptionListener, corruption);
                } finally {
                    CanonicalRandomAccessFiles.releaseExclusiveFileLock(file);
                }
                // We are ready with exclusive access.
                // Demote the lock from exclusive to shared
                CanonicalRandomAccessFiles.acquireSharedFileLock(file, fileChannel);
                commitChronicleMapReady(map, raf, headerBuffer, headerSize);
            }
            map.addCloseable(() -> CanonicalRandomAccessFiles.releaseSharedFileLock(file));
            if (MAP_CREATION_DEBUG) {
                Jvm.warn().on(getClass(), "<map creation debug> Created map [name=" + map.name() +
                        ", size=" + map.longSize() + "] from file [canonizedMapDataFile=" +
                        file.getAbsolutePath() + "]");
            }
            return map;
        } catch (Throwable t) {
            if (recover && !(t instanceof IOException) &&
                    !(t instanceof ChronicleHashRecoveryFailedException)) {
                throw new ChronicleHashRecoveryFailedException(t);
            }
            throw Throwables.propagateNotWrapping(t, IOException.class);
        }
    }

    private ChronicleMap<K, V> createWithoutFile() {
        replicated = replicationIdentifier != -1;
        persisted = false;

        final ChronicleHashResources resources = new InMemoryChronicleHashResources();
        try {
            final VanillaChronicleMap<K, V, ?> map = newMap();
            map.createInMemoryStoreAndSegments(resources);
            prepareMapPublication(map);
            return map;
        } catch (Throwable throwable) {
            try {
                try {
                    resources.setChronicleHashIdentityString(
                            "ChronicleHash{name=" + name + ", file=null}");
                } catch (Throwable t) {
                    throwable.addSuppressed(t);
                } finally {
                    resources.releaseManually();
                }
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw Throwables.propagate(throwable);
        }
    }

    @SuppressWarnings("unchecked")
    private VanillaChronicleMap<K, V, ?> newMap() throws IOException {
        preMapConstruction();
        if (replicated) {
            try {
                return (VanillaChronicleMap<K, V, ?>) Class.forName(replicatedMapClassName).
                        getDeclaredConstructor(getClass()).newInstance(this);
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                    InvocationTargetException | ClassNotFoundException e) {
                throw new IllegalStateException("Cannot load specified implementation class: " + replicatedMapClassName,
                        e);
            }
        } else {
            return new VanillaChronicleMap<>(this);
        }
    }

    private void preMapConstruction() {
        averageKeySize = preMapConstruction(keyBuilder, averageKeySize, averageKey, sampleKey, "Key");
        averageValueSize = preMapConstruction(valueBuilder, averageValueSize, averageValue, sampleValue, "Value");
        if (sparseFile) {
            averageKeySize *= 2;
            averageValueSize *= 2;
        }
        stateChecks();
    }

    private <E> double preMapConstruction(@NotNull final SerializationBuilder<E> builder,
                                          final double configuredAverageSize,
                                          @Nullable final E average,
                                          @Nullable final E sample,
                                          @NotNull final String dim) {
        if (sample != null) {
            return builder.constantSizeBySample(sample);
        } else {
            double result = averageKeyOrValueSize(configuredAverageSize, builder, average);
            if (!isNaN(result) || allLowLevelConfigurationsAreManual()) {
                return result;
            } else {
                throw new IllegalStateException(dim + " size in serialized form must " +
                        "be configured in ChronicleMap, at least approximately.\nUse builder" +
                        ".average" + dim + "()/.constant" + dim + "SizeBySample()/" +
                        ".average" + dim + "Size() methods to configure the size");
            }
        }
    }

    private void stateChecks() {
        checkActualChunksPerSegmentTierIsConfiguredOnlyIfOtherLowLevelConfigsAreManual();
        checkActualChunksPerSegmentGreaterOrEqualToEntries();
    }

    private boolean allLowLevelConfigurationsAreManual() {
        return actualSegments > 0 && entriesPerSegment > 0 && actualChunksPerSegmentTier > 0
                && actualChunkSize > 0;
    }

    private void establishReplication(
            VanillaChronicleMap<K, V, ?> map) {
        if (map instanceof ReplicatedChronicleMap) {
            final ReplicatedChronicleMap result = (ReplicatedChronicleMap) map;
            if (cleanupRemovedEntries)
                establishCleanupThread(result);
        }
    }

    private void establishCleanupThread(@NotNull final ReplicatedChronicleMap map) {
        final OldDeletedEntriesCleanupThread cleanupThread = new OldDeletedEntriesCleanupThread(map);
        map.addCloseable(cleanupThread);
        cleanupThread.start();
    }

    private enum ChecksumEntries {YES, NO, IF_PERSISTED}

    private static final class EntrySizeInfo {
        private final double averageEntrySize;
        private final int worstAlignment;

        EntrySizeInfo(final double averageEntrySize, final int worstAlignment) {
            this.averageEntrySize = averageEntrySize;
            this.worstAlignment = worstAlignment;
        }
    }
}