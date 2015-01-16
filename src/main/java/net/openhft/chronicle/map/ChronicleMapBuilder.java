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

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.*;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.AllocateInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.NewInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.round;
import static net.openhft.chronicle.map.Objects.builderEquals;
import static net.openhft.lang.model.DataValueGenerator.firstPrimitiveFieldType;

/**
 * {@code ChronicleMapBuilder} manages {@link ChronicleMap} configurations; could be used as a
 * classic builder and/or factory. This means that in addition to the standard builder usage
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
 * <p>Later in this documentation, "ChronicleMap" means "ChronicleMaps, created by {@code
 * ChronicleMapBuilder}", unless specified different, because theoretically someone might provide
 * {@code ChronicleMap} implementations with completely different properties.
 *
 * <p>{@code ChronicleMap} ("ChronicleMaps, created by {@code ChronicleMapBuilder}") currently
 * doesn't support resizing. That is why you <i>must</i> configure {@linkplain #entries(long) number
 * of entries} you are going to insert into the created map <i>at most</i>. See {@link
 * #entries(long)} method documentation for more information on this.
 *
 * <p>If you key or value type is not constantly sized and known to {@code ChronicleHashBuilder}, i.
 * e. it is not a boxed primitive, data value generated interface, {@link Byteable}, etc. (see the
 * complete list TODO insert the link to the complete list), you <i>must</i> provide the {@code
 * ChronicleHashBuilder} with some information about you keys or values: if they are
 * constantly-sized, call {@link #constantKeySizeBySample(Object)}, otherwise {@link
 * ChronicleHashBuilder#averageKeySize(double)} method, accordingly for values.
 *
 * @param <K> key type of the maps, produced by this builder
 * @param <V> value type of the maps, produced by this builder
 * @see ChronicleMap
 * @see ChronicleSetBuilder
 */
public final class ChronicleMapBuilder<K, V> implements
        ChronicleHashBuilder<K, ChronicleMap<K, V>, ChronicleMapBuilder<K, V>>,
        MapBuilder<ChronicleMapBuilder<K, V>>, Serializable {

    static final byte UDP_REPLICATION_MODIFICATION_ITERATOR_ID = (byte) 128;
    private static final int DEFAULT_KEY_OR_VALUE_SIZE = 120;
    private static final long DEFAULT_ENTRIES = 1 << 20;
    private static final int MAX_SEGMENTS = (1 << 30);
    private static final Logger LOG =
            LoggerFactory.getLogger(ChronicleMapBuilder.class.getName());

    private static final StringBuilder EMPTY_STRING_BUILDER = new StringBuilder();

    private static final double UNDEFINED_DOUBLE_CONFIG = Double.NaN;

    private static final int XML_SERIALIZATION = 1;
    private static final int BINARY_SERIALIZATION = 2;

    static final long RUNTIME_PAGE_SIZE = (long) NativeBytes.UNSAFE.pageSize();

    private static boolean isDefined(double config) {
        return !Double.isNaN(config);
    }

    private static final boolean strictStateChecks =
            Boolean.getBoolean("chronicle.strictStateChecks");

    SerializationBuilder<K> keyBuilder;
    SerializationBuilder<V> valueBuilder;

    private String name;

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private long entriesPerSegment = -1L;
    private long actualChunksPerSegment = -1L;
    private double averageKeySize = UNDEFINED_DOUBLE_CONFIG;
    private K sampleKey;
    private double averageValueSize = UNDEFINED_DOUBLE_CONFIG;
    private V sampleValue;
    private int actualChunkSize = 0;
    private int maxChunksPerEntry = -1;
    private Alignment alignment = null;
    private long entries = -1L;
    private long lockTimeOut = 20000L;
    private TimeUnit lockTimeOutUnit = TimeUnit.MILLISECONDS;
    private int metaDataBytes = 0;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;

    // replication
    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;
    private V defaultValue = null;
    private DefaultValueProvider<K, V> defaultValueProvider = null;

    private SingleChronicleHashReplication singleHashReplication = null;
    private InetSocketAddress[] pushToAddresses;

    ChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        keyBuilder = new SerializationBuilder<>(keyClass, SerializationBuilder.Role.KEY);
        valueBuilder = new SerializationBuilder<>(valueClass, SerializationBuilder.Role.VALUE);

        if (CharSequence.class == valueClass)
            defaultValue = (V) "";
        if (StringBuilder.class == valueClass)
            defaultValue = (V) EMPTY_STRING_BUILDER;
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
    public static <K, V> ChronicleMapBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
        return new ChronicleMapBuilder<>(keyClass, valueClass);
    }

    public static <K, V> ChronicleMapStatelessClientBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass, InetSocketAddress socketAddress) {
        return ChronicleMapStatelessClientBuilder.of(socketAddress);
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
     * @param averageKeySize
     * @throws IllegalStateException    {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @see #constantKeySizeBySample(Object)
     * @see #averageValueSize(double)
     * @see #actualChunkSize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> averageKeySize(double averageKeySize) {
        checkSizeIsNotStaticallyKnown(keyBuilder);
        checkAverageSize(averageKeySize, "key");
        this.averageKeySize = averageKeySize;
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
     * @see ChronicleHashBuilder#averageKeySize(double)
     * @see #constantValueSizeBySample(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> constantKeySizeBySample(K sampleKey) {
        this.sampleKey = sampleKey;
        return this;
    }

    private double averageKeySize() {
        return averageKeyOrValueSize(averageKeySize, keyBuilder);
    }

    /**
     * Configures the average number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. If value size is always the same, call {@link
     * #constantValueSizeBySample(Object)} method instead of this one.
     *
     * <p>{@code ChronicleHashBuilder} implementation heuristically chooses {@linkplain
     * #actualChunkSize(int) the actual chunk size} based on this configuration and the key size,
     * that, however, might result to quite high internal fragmentation, i. e. losses because only
     * integral number of chunks could be allocated for the entry. If you want to avoid this, you
     * should manually configure the actual chunk size in addition to this average value size
     * configuration, which is anyway needed.
     *
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and shouldn't be specified by user.
     *
     * @param averageValueSize number of bytes, taken by serialized form of values
     * @return this builder back
     * @throws IllegalStateException    if value size is known statically and shouldn't be
     *                                  configured by user
     * @throws IllegalArgumentException if the given {@code averageValueSize} is non-positive
     * @see #constantValueSizeBySample(Object)
     * @see ChronicleHashBuilder#averageKeySize(double)
     * @see #actualChunkSize(int)
     */
    public ChronicleMapBuilder<K, V> averageValueSize(double averageValueSize) {
        checkSizeIsNotStaticallyKnown(valueBuilder);
        checkAverageSize(averageValueSize, "value");
        this.averageValueSize = averageValueSize;
        return this;
    }

    private static void checkAverageSize(double averageSize, String role) {
        if (averageSize <= 0 || Double.isNaN(averageSize) ||
                Double.isInfinite(averageSize)) {
            throw new IllegalArgumentException("Average " + role + " size must be a positive, " +
                    "finite number");
        }
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
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If value size varies, method {@link #averageValueSize(double)} should be called instead of
     * this one.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #averageValueSize(double)
     * @see #constantKeySizeBySample(Object)
     */
    public ChronicleMapBuilder<K, V> constantValueSizeBySample(V sampleValue) {
        this.sampleValue = sampleValue;
        return this;
    }

    double averageValueSize() {
        return averageKeyOrValueSize(averageValueSize, valueBuilder);
    }

    private double averageKeyOrValueSize(double configuredSize, SerializationBuilder builder) {
        if (isDefined(configuredSize))
            return configuredSize;
        if (builder.constantSizeMarshaller())
            return builder.pseudoReadConstantSize();
        return DEFAULT_KEY_OR_VALUE_SIZE;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException is sizes of both keys and values of maps created by this
     *                               builder are constant, hence chunk size shouldn't be configured
     *                               by user
     * @see #entryAndValueAlignment(Alignment)
     * @see #entries(long)
     * @see #maxChunksPerEntry(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> actualChunkSize(int actualChunkSize) {
        if (constantlySizedEntries()) {
            throw new IllegalStateException("Sizes of key type: " + keyBuilder.eClass + " and " +
                    "value type: " + valueBuilder.eClass + " are both constant, " +
                    "so chunk size shouldn't be specified manually");
        }
        if (actualChunkSize <= 0)
            throw new IllegalArgumentException("Chunk size must be positive");
        this.actualChunkSize = actualChunkSize;
        return this;
    }

    static class EntrySizeInfo {
        final double averageEntrySize;
        final int worstAlignment;

        public EntrySizeInfo(double averageEntrySize, int worstAlignment) {
            this.averageEntrySize = averageEntrySize;
            this.worstAlignment = worstAlignment;
        }
    }

    private EntrySizeInfo entrySizeInfo(boolean replicated) {
        double size = metaDataBytes;
        double keySize = averageKeySize();
        size += averageSizeEncodingSize(keyBuilder, keySize);
        size += keySize;
        if (replicated)
            size += ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;
        double valueSize = averageValueSize();
        size += averageSizeEncodingSize(valueBuilder, valueSize);
        Alignment alignment = valueAlignment();
        int worstAlignment;
        if (alignment != Alignment.NO_ALIGNMENT &&
                constantlySizedKeys() && valueBuilder.constantSizeEncodingSizeMarshaller()) {
            long constantSizeBeforeAlignment = round(size);
            if (constantlySizedValues()) {
                // see segmentEntrySpaceInnerOffset()
                long totalDataSize = constantSizeBeforeAlignment + constantValueSize();
                worstAlignment = (int) (alignment.alignAddr(totalDataSize) - totalDataSize);
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
                            maxDefaultChunksPerAverageEntry(replicated) * chunkSize) {
                        break determineAlignment;
                    }
                    chunkSize = 4;
                    worstAlignment = worstAlignmentAssumingChunkSize(
                            constantSizeBeforeAlignment, chunkSize);
                }
            }
        } else {
            // assume worst case, we always lose most possible bytes for alignment
            worstAlignment = alignment.alignment() - 1;
        }
        size += worstAlignment;
        size += valueSize;
        return new EntrySizeInfo(size, worstAlignment);
    }

    /**
     * This is needed, if chunkSize = constant entry size is not aligned, for entry alignment to be
     * always the same, we should _misalign_ the first chunk.
     */
    int segmentEntrySpaceInnerOffset(boolean replicated) {
        if (!constantlySizedEntries())
            return 0;
        return (int) (constantValueSize() % valueAlignment().alignment());
    }

    private long constantValueSize() {
        return valueBuilder.pseudoReadConstantSize();
    }

    private boolean constantlySizedKeys() {
        return keyBuilder.constantSizeMarshaller();
    }

    private static double averageSizeEncodingSize(
            SerializationBuilder builder, double averageSize) {
        SizeMarshaller sizeMarshaller = builder.sizeMarshaller();
        if (averageSize == round(averageSize))
            return sizeMarshaller.sizeEncodingSize(round(averageSize));
        long lower = (long) averageSize;
        long upper = lower + 1;
        int lowerEncodingSize = sizeMarshaller.sizeEncodingSize(lower);
        int upperEncodingSize = sizeMarshaller.sizeEncodingSize(upper);
        if (lowerEncodingSize == upperEncodingSize)
            return lowerEncodingSize;
        return lower * (upper - averageSize) + upper * (averageSize - lower);
    }

    private int worstAlignmentAssumingChunkSize(
            long constantSizeBeforeAlignment, int chunkSize) {
        Alignment valueAlignment = valueAlignment();
        long firstAlignment = valueAlignment.alignAddr(constantSizeBeforeAlignment) -
                constantSizeBeforeAlignment;
        int alignment = valueAlignment.alignment();
        int gcdOfAlignmentAndChunkSize = greatestCommonDivisor(alignment, chunkSize);
        if (gcdOfAlignmentAndChunkSize == alignment)
            return (int) firstAlignment;
        // assume worst by now because we cannot predict alignment in VanillaCM.entrySize() method
        // before allocation
        long worstAlignment = firstAlignment;
        while (worstAlignment + gcdOfAlignmentAndChunkSize < alignment)
            worstAlignment += gcdOfAlignmentAndChunkSize;
        return (int) worstAlignment;
    }

    int worstAlignment(boolean replicated) {
        return entrySizeInfo(replicated).worstAlignment;
    }

    static int greatestCommonDivisor(int a, int b) {
        if (b == 0) return a;
        return greatestCommonDivisor(b, a % b);
    }

    long chunkSize(boolean replicated) {
        if (actualChunkSize > 0)
            return actualChunkSize;
        double averageEntrySize = entrySizeInfo(replicated).averageEntrySize;
        if (constantlySizedEntries())
            return round(averageEntrySize);
        int maxChunkSize = 1 << 30;
        for (long chunkSize = 4; chunkSize <= maxChunkSize; chunkSize *= 2L) {
            if (maxDefaultChunksPerAverageEntry(replicated) * chunkSize > averageEntrySize)
                return chunkSize;
        }
        return maxChunkSize;
    }

    boolean constantlySizedEntries() {
        return constantlySizedKeys() && constantlySizedValues();
    }

    double averageChunksPerEntry(boolean replicated) {
        if (constantlySizedEntries())
            return 1.0;
        long chunkSize = chunkSize(replicated);
        // assuming we always has worst internal fragmentation. This affects total segment
        // entry space which is allocated lazily on Linux (main target platform)
        // so we can afford this
        return (entrySizeInfo(replicated).averageEntrySize + chunkSize - 1) / chunkSize;
    }

    private static int maxDefaultChunksPerAverageEntry(boolean replicated) {
        return replicated ? 4 : 8;
    }

    @Override
    public ChronicleMapBuilder<K, V> maxChunksPerEntry(int maxChunksPerEntry) {
        if (maxChunksPerEntry < 1)
            throw new IllegalArgumentException("maxChunksPerEntry should be >= 1, " +
                    maxChunksPerEntry + " given");
        this.maxChunksPerEntry = maxChunksPerEntry;
        return this;
    }

    int maxChunksPerEntry() {
        if (maxChunksPerEntry > 0)
            return maxChunksPerEntry;
        if (constantlySizedEntries()) {
            return 1;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private boolean constantlySizedValues() {
        return valueBuilder.constantSizeMarshaller();
    }

    /**
     * Configures alignment strategy of address in memory of entries and independently of address in
     * memory of values within entries in ChronicleMaps, created by this builder.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with volatile
     * access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>If values couldn't reference off-heap memory (i. e. it is not {@link Byteable} or "data
     * value generated"), alignment configuration makes no sense and forbidden.
     *
     * <p>Default is {@link Alignment#NO_ALIGNMENT} if values couldn't reference off-heap memory,
     * otherwise chosen heuristically (configure explicitly for being sure and to compare
     * performance in your case).
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     * @throws IllegalStateException if values of maps, created by this builder, couldn't reference
     *                               off-heap memory
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

    Alignment valueAlignment() {
        if (alignment != null)
            return alignment;
        Class firstPrimitiveFieldType = firstPrimitiveFieldType(valueBuilder.eClass);
        if (firstPrimitiveFieldType == long.class || firstPrimitiveFieldType == double.class)
            return Alignment.OF_8_BYTES;
        if (firstPrimitiveFieldType == int.class || firstPrimitiveFieldType == float.class)
            return Alignment.OF_4_BYTES;
        return Alignment.NO_ALIGNMENT;
    }

    @Override
    public ChronicleMapBuilder<K, V> entries(long entries) {
        if (entries <= 0L)
            throw new IllegalArgumentException("Entries should be positive, " + entries + " given");
        this.entries = entries;
        return this;
    }

    long entries() {
        if (entries < 0)
            return DEFAULT_ENTRIES;
        return entries;
    }

    @Override
    public ChronicleMapBuilder<K, V> entriesPerSegment(long entriesPerSegment) {
        if (entriesPerSegment <= 0L)
            throw new IllegalArgumentException("Entries per segment should be positive, " +
                    entriesPerSegment + " given");
        this.entriesPerSegment = entriesPerSegment;
        return this;
    }

    long entriesPerSegment(boolean replicated) {
        long entriesPerSegment;
        if (this.entriesPerSegment > 0L) {
            entriesPerSegment = this.entriesPerSegment;
        } else {
            int actualSegments = actualSegments(replicated);
            long totalEntries = totalEntriesIfPoorDistribution(actualSegments);
            entriesPerSegment = divideUpper(totalEntries, actualSegments);
        }
        if (actualChunksPerSegment > 0)
            return entriesPerSegment;
        double averageChunksPerEntry = averageChunksPerEntry(replicated);
        if (entriesPerSegment * averageChunksPerEntry > HashLookup.MAX_SEGMENT_CHUNKS)
            throw new IllegalStateException("Max chunks per segment is " +
                    HashLookup.MAX_SEGMENT_CHUNKS + " configured entries() and " +
                    "actualSegments() so that there should be " + entriesPerSegment +
                    " entries per segment, while average chunks per entry is " +
                    averageChunksPerEntry);
        if (entriesPerSegment > HashLookup.MAX_SEGMENT_ENTRIES)
            throw new IllegalStateException("shouldn't be more than " +
                    HashLookup.MAX_SEGMENT_ENTRIES + " entries per segment");
        return entriesPerSegment;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualChunksPerSegment(long actualChunksPerSegment) {
        if (actualChunksPerSegment <= 0)
            throw new IllegalArgumentException("Actual chunks per segment should be positive, " +
                    actualChunksPerSegment + " given");
        this.actualChunksPerSegment = actualChunksPerSegment;
        return this;
    }

    private void checkActualChunksPerSegmentIsConfiguredOnlyIfOtherLowLevelConfigsAreManual() {
        if (actualChunksPerSegment > 0) {
            if (entriesPerSegment <= 0 || (actualChunkSize <= 0 && !constantlySizedEntries()) ||
                    actualSegments <= 0)
                throw new IllegalStateException("Actual chunks per entry could be configured " +
                        "only if other three low level configs are manual: " +
                        "entriesPerSegment(), actualSegments() and actualChunkSize(), unless " +
                        "both keys and value sizes are constant");
        }
    }

    private void checkActualChunksPerSegmentGreaterOrEqualToEntries() {
        if (actualChunksPerSegment > 0 && entriesPerSegment > 0 &&
                entriesPerSegment > actualChunksPerSegment) {
            throw new IllegalStateException("Entries per segment couldn't be greater than " +
                    "actual chunks per segment. Entries: " + entriesPerSegment + ", " +
                    "chunks: " + actualChunksPerSegment + " is configured");
        }
    }

    long actualChunksPerSegment(boolean replicated) {
        if (actualChunksPerSegment > 0)
            return actualChunksPerSegment;
        return round(entriesPerSegment(replicated) * averageChunksPerEntry(replicated));
    }

    private long totalEntriesIfPoorDistribution(int segments) {
        if (segments == 1)
            return entries();
        long entries = entries();
        // check if the number of entries is small compared with the number of segments.
        long s3 = (long) Math.min(8, segments) * Math.min(32, segments) * Math.min(128, segments);
        if (entries * 4 <= s3)
            entries *= 1.8;
        else if (entries <= s3)
            entries *= 1.45;
        else if (entries <= s3 * 4)
            entries *= 1.33;
        else if (entries <= s3 * 8)
            entries *= 1.22;
        else if (entries <= s3 * 16)
            entries *= 1.15;
        else
            entries *= 1.1;
        return Math.min(segments * entries(),
                entries + 4 * segments + 8);
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
        return (int) Math.min(Maths.nextPower2(entries() / 32, 1), estimateSegmentsBasedOnSize());
    }

    //TODO reivew because this heuristic doesn't seem to perform well
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

    private static int estimateSegmentsForEntries(long size) {
        if (size > 200 << 20)
            return 256;
        if (size >= 1 << 20)
            return 128;
        if (size >= 128 << 10)
            return 64;
        if (size >= 16 << 10)
            return 32;
        if (size >= 4 << 10)
            return 16;
        if (size >= 1 << 10)
            return 8;
        return 1;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        checkSegments(actualSegments);
        this.actualSegments = actualSegments;
        return this;
    }

    int actualSegments(boolean replicated) {
        if (actualSegments > 0)
            return actualSegments;
        if (entriesPerSegment > 0) {
            long segments = 1;
            for (int i = 0; i < 3; i++) { // try to converge
                long totalEntries = totalEntriesIfPoorDistribution((int) segments);
                segments = divideUpper(totalEntries, entriesPerSegment);
                if (segments > MAX_SEGMENTS)
                    throw new IllegalStateException();
            }
            if (minSegments > 0)
                segments = Math.max(minSegments, segments);
            return (int) segments;
        }
        long smallSegments = trySegments(HashLookup.SMALL_SEGMENT_CHUNKS, MAX_SEGMENTS, replicated);
        if (smallSegments > 0L)
            return (int) smallSegments;
        long intMMapSegments = trySegments(HashLookup.MAX_SEGMENT_CHUNKS, MAX_SEGMENTS, replicated);
        if (intMMapSegments > 0L)
            return (int) intMMapSegments;
        throw new IllegalStateException("Max segments is " + MAX_SEGMENTS + ", configured so much" +
                " entries (" + entries() + ") or average chunks per entry is too high (" +
                averageChunksPerEntry(replicated) + ") that builder automatically decided to use " +
                (-intMMapSegments) + " segments");
    }

    private long trySegments(long maxSegmentCapacity, int maxSegments, boolean replicated) {
        long totalChunks = round(totalEntriesIfPoorDistribution(minSegments()) *
                averageChunksPerEntry(replicated));
        long segments = divideUpper(totalChunks, maxSegmentCapacity);
        segments = Maths.nextPower2(Math.max(segments, minSegments()), 1L);
        return segments <= maxSegments ? segments : -segments;
    }

    int segmentHeaderSize(boolean replicated) {
        int segments = actualSegments(replicated);
        // reduce false sharing unless we have a lot of segments.
        return segments <= 16 * 1024 ? 64 : 32;
    }

    @Override
    public ChronicleMapBuilder<K, V> putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    @Override
    public ChronicleMapBuilder<K, V> removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    // hidden for initial release.
    ChronicleMapBuilder<K, V> metaDataBytes(int metaDataBytes) {
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
                ", entriesPerSegment=" + pretty(entriesPerSegment) +
                ", actualChunksPerSegment=" + pretty(actualChunksPerSegment) +
                ", averageKeySize=" + pretty(averageKeySize) +
                ", sampleKeyForConstantSizeComputation=" + pretty(sampleKey) +
                ", averageValueSize=" + pretty(averageValueSize) +
                ", sampleValueForConstantSizeComputation=" + pretty(sampleValue) +
                ", actualChunkSize=" + pretty(actualChunkSize) +
                ", valueAlignment=" + valueAlignment() +
                ", entries=" + entries() +
                ", lockTimeOut=" + lockTimeOut + " " + lockTimeOutUnit +
                ", metaDataBytes=" + metaDataBytes() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerFactory=" + pretty(bytesMarshallerFactory) +
                ", objectSerializer=" + pretty(objectSerializer) +
                ", keyBuilder=" + keyBuilder +
                ", valueBuilder=" + valueBuilder +
                ", defaultValue=" + defaultValue +
                ", defaultValueProvider=" + pretty(defaultValueProvider) +
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
     * {@inheritDoc} <p>
     * <p>Example: <pre>{@code Map<Key, Value> map =
     *     ChronicleMapBuilder.of(Key.class, Value.class)
     *     .entries(1_000_000)
     *     .averageKeySize(50).averageValueSize(200)
     *     // this class hasn't implemented yet, just for example
     *     .objectSerializer(new KryoObjectSerializer())
     *     .create();}</pre>
     * <p> <p>This serializer is used to serialize both keys and values, if they both require this:
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
            @NotNull BytesWriter<? super K> keyWriter, @NotNull BytesReader<K> keyReader) {
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
            @NotNull ObjectFactory<? extends K> keyDeserializationFactory) {
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
     * memory in maps, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
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
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory in
     * maps, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information. <p>
     * <p>Configuring marshalling this way results to a little bit more compact in-memory layout of
     * the map, comparing to a single interface configuration: {@link #valueMarshaller(
     *BytesMarshaller)}. <p> <p>Passing {@link BytesInterop} instead of plain {@link BytesWriter}
     * is, of cause, possible, but currently pointless for values.
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
     * <p>Default value size marshaller is so-called "stop bit encoding" marshalling, unless {@link
     * #constantValueSizeBySample(Object)} or the builder statically knows the value size is
     * constant -- special constant size marshalling is used by default in these cases.
     *
     * @param valueSizeMarshaller the new marshaller, used to serialize actual value sizes to
     *                            off-heap memory
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
     * {@link DeserializationFactoryConfigurableBytesReader}, in maps, created by this builder. <p>
     * <p>Default value deserialization factory is {@link NewInstanceObjectFactory}, which creates a
     * new value instance using {@link Class#newInstance()} default constructor. You could provide
     * an {@link AllocateInstanceObjectFactory}, which uses {@code Unsafe.allocateInstance(Class)}
     * (you might want to do this for better performance or if you don't want to initialize fields),
     * or a factory which calls a value class constructor with some arguments, or a factory which
     * internally delegates to instance pool or {@link ThreadLocal}, to reduce allocations.
     *
     * @param valueDeserializationFactory the value factory used to produce instances to deserialize
     *                                    data in
     * @return this builder back
     * @throws IllegalStateException if it is not possible to apply deserialization factory to value
     *                               deserializers, currently configured for this builder
     * @see #keyDeserializationFactory(ObjectFactory)
     */
    public ChronicleMapBuilder<K, V> valueDeserializationFactory(
            @NotNull ObjectFactory<V> valueDeserializationFactory) {
        valueBuilder.factory(valueDeserializationFactory);
        return this;
    }

    /**
     * Specifies the value to be put for each key queried in {@link ChronicleMap#acquireUsing
     * acquireUsing()} method, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValueProvider(
     * DefaultValueProvider)} configuration to this builder.
     *
     * @param defaultValue the default value to be put to the map for absent keys during {@code
     *                     acquireUsing()} calls
     * @return this builder object back
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        if (defaultValue == null)
            throw new IllegalArgumentException("default ChronicleMap value couldn't be null");
        this.defaultValue = defaultValue;
        this.defaultValueProvider = null;
        return this;
    }

    /**
     * Specifies the function to obtain a value for the key during {@link ChronicleMap#acquireUsing
     * acquireUsing()} calls, if the key is absent in the map, created by this builder.
     *
     * <p>This configuration overrides any previous {@link #defaultValue(Object)} configuration
     * to this builder.
     *
     * @param defaultValueProvider the strategy to obtain a default value by the absent key
     * @return this builder object back
     * @see #defaultValue(Object)
     */
    public ChronicleMapBuilder<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        this.defaultValue = null;
        return this;
    }

    /**
     * Non-public because should be called only after {@link #preMapConstruction(boolean)}
     */
    DefaultValueProvider<K, V> defaultValueProvider() {
        if (defaultValueProvider != null)
            return defaultValueProvider;
        V defaultValue = this.defaultValue;
        if (defaultValue == null)
            defaultValue = zeroValue();
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
        return new ConstantValueProvider<>(defaultValue, metaValueWriter, valueWriter);
    }

    private V zeroValue() {
        if (Byteable.class.isAssignableFrom(valueBuilder.eClass)) {
            try {
                Object v = NativeBytes.UNSAFE.allocateInstance(valueBuilder.eClass);
                Byteable defaultValue = (Byteable) v;
                defaultValue.bytes(DirectStore.allocate(defaultValue.maxSize()).bytes(), 0);
                return (V) v;
            } catch (InstantiationException e) {
                return null;
            }
        } else {
            try {
                return DataValueClasses.newDirectInstance(valueBuilder.eClass);
            } catch (Exception e) {
                return null;
            }
        }
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
    public ChronicleHashInstanceBuilder<ChronicleMap<K, V>> instance() {
        return new MapInstanceBuilder<>(this.clone(), singleHashReplication, null, null, null,
                new AtomicBoolean(false));
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

    ChronicleMap<K, V> create(MapInstanceBuilder<K, V> ib) throws IOException {
        if (ib.file != null) {
            return createWithFile(ib.file, ib.singleHashReplication, ib.channel);
        } else {
            return createWithoutFile(ib.singleHashReplication, ib.channel);
        }
    }

    ChronicleMap<K, V> createWithFile(File file, SingleChronicleHashReplication singleHashReplication,
                                      ReplicationChannel channel) throws IOException {
        // pushingToMapEventListener();
        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                try (FileInputStream fis = new FileInputStream(file);
                     ObjectInputStream ois = new ObjectInputStream(fis)) {
                    Object m;
                    byte serialization = ois.readByte();
                    if (serialization == XML_SERIALIZATION) {
                        m = deserializeHeaderViaXStream(ois);
                    } else if (serialization == BINARY_SERIALIZATION) {
                        try {
                            m = ois.readObject();
                        } catch (ClassNotFoundException e) {
                            throw new AssertionError(e);
                        }
                    } else {
                        throw new IOException("Unknown map header serialization type: " +
                                serialization);
                    }
                    VanillaChronicleMap<K, ?, ?, V, ?, ?> map =
                            (VanillaChronicleMap<K, ?, ?, V, ?, ?>) m;
                    map.headerSize = roundUpMapHeaderSize(fis.getChannel().position());
                    map.createMappedStoreAndSegments(file);
                    // This is needed to property initialize key and value serialization builders,
                    // which are later used in replication
                    // TODO don't use SerializationBuilders in replication, extract marshallers
                    // needed to transmit to stateless clients directly from map instance
                    preMapConstruction(singleHashReplication != null || channel != null);
                    return establishReplication(map, singleHashReplication, channel);
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
            if (!trySerializeHeaderViaXStream(map, oos)) {
                oos.writeByte(BINARY_SERIALIZATION);
                oos.writeObject(map);
            }
            oos.flush();
            map.headerSize = roundUpMapHeaderSize(fos.getChannel().position());
            map.createMappedStoreAndSegments(file);
        }

        return establishReplication(map, singleHashReplication, channel);
    }

    private static <K, V> boolean trySerializeHeaderViaXStream(
            VanillaChronicleMap<K, ?, ?, V, ?, ?> map, ObjectOutputStream oos) throws IOException {
        Class<?> xStreamClass;
        try {
            xStreamClass =
                    Class.forName("net.openhft.xstream.MapHeaderSerializationXStream");
        } catch (ClassNotFoundException e) {
            xStreamClass = null;
        }
        if (xStreamClass == null) {
            LOG.info("xStream not found, use binary ChronicleMap header serialization");
            return false;
        }
        try {
            oos.writeByte(XML_SERIALIZATION);
            Method toXML = xStreamClass.getMethod("toXML", Object.class, OutputStream.class);
            toXML.invoke(xStreamClass.newInstance(), map, oos);
            return true;
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                InstantiationException e) {
            throw new AssertionError(e);
        }
    }

    private static Object deserializeHeaderViaXStream(ObjectInputStream ois) {
        try {
            Class<?> xStreamClass =
                    Class.forName("net.openhft.xstream.MapHeaderSerializationXStream");
            Method fromXML = xStreamClass.getMethod("fromXML", InputStream.class);
            return fromXML.invoke(xStreamClass.newInstance(), ois);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                IllegalAccessException | InstantiationException e) {
            throw new AssertionError(e);
        }
    }

    ChronicleMap<K, V> createWithoutFile(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel) {
        try {
            // pushingToMapEventListener();
            VanillaChronicleMap<K, ?, ?, V, ?, ?> map = newMap(singleHashReplication, channel);
            map.warnOnWindows();
            BytesStore bytesStore = new DirectStore(JDKObjectSerializer.INSTANCE,
                    map.sizeInBytes(), true);
            map.createMappedStoreAndSegments(bytesStore);
            return establishReplication(map, singleHashReplication, channel);
        } catch (IOException e) {
            // file-less version should never trigger an IOException.
            throw new AssertionError(e);
        }
    }
//
//    private void pushingToMapEventListener() {
//        if (pushToAddresses == null || pushToAddresses.length == 0) {
//            return;
//        }
//        try {
//            Class<?> pmel = Class.forName(
//                    "com.higherfrequencytrading.chronicle.enterprise.map.PushingMapEventListener");
//            Constructor<?> constructor = pmel.getConstructor(ChronicleMap[].class);
//            // create a stateless client for each address
//            ChronicleMap[] statelessClients = new ChronicleMap[pushToAddresses.length];
//            ChronicleMapBuilder<K, V> cmb = clone();
//            cmb.pushTo((InetSocketAddress[]) null);
//            for (int i = 0; i < pushToAddresses.length; i++) {
//                statelessClients[i] = ChronicleMapStatelessClientBuilder.of(pushToAddresses[i])
//                        .create();
//            }
//            eventListener =
//                    (MapEventListener<K, V>) constructor.newInstance((Object) statelessClients);
//        } catch (ClassNotFoundException e) {
//            LoggerFactory.getLogger(getClass().getName())
//                    .warn("Chronicle Enterprise not found in the class path");
//        } catch (Exception e) {
//            LoggerFactory.getLogger(getClass().getName())
//                    .error("PushingMapEventListener failed to load", e);
//        }
//    }

    private VanillaChronicleMap<K, ?, ?, V, ?, ?> newMap(
            SingleChronicleHashReplication singleHashReplication, ReplicationChannel channel)
            throws IOException {
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

        double largeKeySize = averageKeySize();
        if (!constantlySizedKeys())
            largeKeySize *= figureBufferAllocationFactor();
        keyBuilder.maxSize(round(largeKeySize));
        double largeValueSize = averageValueSize();
        if (!constantlySizedValues())
            largeValueSize *= figureBufferAllocationFactor();
        valueBuilder.maxSize(round(largeValueSize));

        if (sampleKey != null)
            keyBuilder.constantSizeBySample(sampleKey);
        if (sampleValue != null)
            valueBuilder.constantSizeBySample(sampleValue);
        stateChecks();
    }

    private void stateChecks() {
        if (strictStateChecks) {
            if (entries < 0)
                throw new IllegalStateException("Entries must be specified");
            if (!constantlySizedKeys() && !isDefined(averageKeySize))
                throw new IllegalStateException("No info about key size");
            if (!constantlySizedValues() && !isDefined(averageValueSize))
                throw new IllegalStateException("No info about value size");
        }
        checkAlignmentOnlyIfValuesPossiblyReferenceOffHeap();
        checkActualChunksPerSegmentIsConfiguredOnlyIfOtherLowLevelConfigsAreManual();
        checkActualChunksPerSegmentGreaterOrEqualToEntries();
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
                Math.min(64, maxChunksPerEntry()));
    }
}

