package net.openhft.chronicle;

import net.openhft.chronicle.map.*;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.jetbrains.annotations.NotNull;

import net.openhft.lang.io.serialization.*;
import net.openhft.lang.model.Byteable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines the meaning of configurations, common to {@link ChronicleMapBuilder}
 * and {@link ChronicleSetBuilder}, i. e. <i>Chronicle hash container</i> configurations.
 *
 * @param <K> the type of keys in hash containers, created by this builder
 * @param <C> the container type, created by this builder, i. e. {@link ChronicleMap}
 *            or {@link ChronicleSet}
 * @param <B> the concrete builder type, i. e. {@link ChronicleMapBuilder}
 *            or {@link ChronicleSetBuilder}
 */
public interface ChronicleHashBuilder<K, C, B extends ChronicleHashBuilder<K, C, B>>
        extends Cloneable {

    B clone();

    /**
     * Set minimum number of segments. See concurrencyLevel in {@link ConcurrentHashMap}.
     *
     * @param minSegments the minimum number of segments in containers, constructed by this builder
     * @return this builder object back
     */
    B minSegments(int minSegments);

    /**
     * Configures the optimal number of bytes, taken by serialized form of keys, put into hash
     * containers, created by this builder. If key size is always the same, call
     * {@link #constantKeySizeBySample(Object)} method instead of this one.
     *
     * <p>If key size varies moderately, specify the size higher than average, but lower than
     * the maximum possible, to minimize average memory overuse. If key size varies in a wide range,
     * it's better to use {@linkplain #entrySize(int) entry size} in "chunk" mode and configure it
     * directly.
     *
     * @param keySize number of bytes, taken by serialized form of keys
     * @return this builder back
     * @see #constantKeySizeBySample(Object)
     * @see #entrySize(int)
     */
    B keySize(int keySize);

    /**
     * Configures the constant number of bytes, taken by serialized form of keys, put into hash
     * containers, created by this builder. This is done by providing the {@code sampleKey},
     * all keys should take the same number of bytes in serialized form, as this sample object.
     *
     * <p>If keys are of boxed primitive type or {@link Byteable} subclass, i. e. if key size
     * is known statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If key size varies, method {@link #keySize(int)} or {@link #entrySize(int)} should be
     * called instead of this one.
     *
     * @param sampleKey the sample key
     * @return this builder back
     * @see #keySize(int)
     */
    B constantKeySizeBySample(K sampleKey);

    /**
     * Configures the size in bytes of allocation unit of hash container instances, created by
     * this builder.
     *
     * <p>{@link ChronicleMap} and {@link ChronicleSet} store their data off-heap, so it is required
     * to serialize key (and values, in {@code ChronicleMap} case) (unless they are direct
     * {@link Byteable} instances). Serialized key bytes (+ serialized value bytes,
     * in {@code ChronicleMap} case) + some metadata bytes comprise "entry space", which
     * {@code ChronicleMap} or {@code ChronicleSet} should allocate. So <i>entry size</i>
     * is a minimum allocation portion in the hash containers, created by this builder.
     * E. g. if entry size is 100, the created container could only allocate 100, 200, 300... bytes
     * for an entry. If say 150 bytes of entry space are required by the entry, 200 bytes
     * will be allocated, 150 used and 50 wasted. To minimize memory overuse and improve speed, you
     * should pay decent attention to this configuration.
     *
     * <p>There are three major patterns of this configuration usage:
     * <ol>
     *     <li>Key (and value, in {@code ChronicleMap} case) sizes are constant. Configure them
     *     via {@link #constantKeySizeBySample(Object)} and
     *     {@link ChronicleMapBuilder#constantValueSizeBySample(Object)} methods, and you will
     *     experience no memory waste at all.</li>
     *     <li>Key (and/or value size, in {@code ChronicleMap} case) varies moderately. Specify them
     *     using corresponding methods, or specify entry size directly by calling this method,
     *     by sizes somewhere between average and maximum possible. The idea is to have most
     *     (90% or more) entries to fit a single "entry size" with moderate memory waste
     *     (10-20% on average), rest 10% or less of  entries should take 2 "entry sizes", thus with
     *     ~50% memory overuse.</li>
     *     <li>Key (and/or value size, in {@code ChronicleMap} case) varies in a wide range.
     *     Then it's best to use entry size configuration in <i>chunk mode</i>. Specify entry size
     *     so that most entries should take from 5 to several dozens of "chunks". With this
     *     approach, average memory waste should be very low.
     *
     *     <p>However, remember that
     *     <ul>
     *         <li>Operations with entries that span several "entry sizes" are a bit slower, than
     *         entries which take a single "entry size". That is why "chunk" approach
     *         is not recommended, when key (and/or value size) varies moderately.</li>
     *         <li>The maximum number of chunks could be taken by an entry is 64.
     *         {@link IllegalArgumentException} is thrown on attempt to insert too large entry,
     *         compared to the configured or computed entry size.</li>
     *         <li>The number of "entries" {@linkplain #entries(long) in the whole map} and
     *         {@linkplain #actualEntriesPerSegment(int) per segment} is actually the number
     *         "chunks".</li>
     *     </ul>
     *
     *     <p>Example: if values in your {@code ChronicleMap} are adjacency lists of some social
     *     graph, where nodes are represented as {@code long} ids, and adjacency lists
     *     are serialized in efficient manner, for example as {@code long[]} arrays. Typical number
     *     of connections is 100-300, maximum is 3000. In this case entry size of
     *     50 * (8 bytes for each id) = 400 bytes would be a good choice: <pre>{@code
     * Map<Long, long[]> socialGraph = ChronicleMapBuilder
     *     .of(Long.class, long[].class)
     *     // given that graph should have of 1 billion nodes, and 150 average adjacency list size
     *     // => values takes 3 chuncks on average
     *     .entries(1_000_000_000L * (150 / 50))
     *     .entrySize(50 * 8)
     *     .create();}</pre>
     *     It is minimum possible (because 3000 friends / 50 friends = 60 is close to 64 "max
     *     chunks by single entry" limit, and ensures moderate average memory overuse (not more
     *     than 20%).
     *     </li>
     * </ol>
     *
     * @param entrySize the "chunk size" in bytes
     * @return this builder back
     * @see #entries(long)
     */
    B entrySize(int entrySize);

    /**
     * Configures the maximum number of {@linkplain #entrySize(int) "entry size chunks"}, which
     * could be taken by the maximum number of entries, inserted into the hash containers, created
     * by this builder. If you try to insert more data, {@link IllegalStateException} might be
     * thrown, because currently {@link ChronicleMap} and {@link ChronicleSet} doesn't support
     * resizing.
     *
     * <ol>
     *     <li>If key size (and value size, in {@code ChronicleMap} case) is constant, this number
     *     is equal to the maximum number of entries (because each entry takes exactly one
     *     "entry size" memory unit).</li>
     *     <li>If key (and/or value, in {@code ChronicleMap} case) size varies moderately, you
     *     should pass to this method the maximum number of entries + 5-25%, depending on your data
     *     properties and configured {@linkplain #keySize(int) key}/{@linkplain
     *     ChronicleMapBuilder#valueSize(int) value}/{@linkplain #entrySize(int) entry} sizes.</li>
     *     <li>If your data size varies in a wide range, pass the maximum number of entries
     *     multiplied by average data size and divided by the configured "entry size" (i. e. chunk
     *     size). See an example in the documentation to {@link #entrySize(int)} method.</li>
     * </ol>
     *
     * <p>You shouldn't put additional margin over the number, computed according the rules above.
     * This bad practice was popularized by {@link HashMap#HashMap(int)}
     * and {@link HashSet#HashSet(int)} constructors, which accept "capacity", that should
     * be multiplied by "load factor" to obtain actual maximum expected number of entries.
     * {@code ChronicleMap} and {@code ChronicleSet} don't have a notion of load factor.
     *
     * <p>Default value is 2^20 (~ 1 million).
     *
     * @param entries maximum size of the created maps, in memory allocation units,
     *                so-called "entry size"
     * @return this builder back
     * @see #entrySize(int)
     */
    B entries(long entries);

    B actualEntriesPerSegment(int actualEntriesPerSegment);

    B actualSegments(int actualSegments);

    B lockTimeOut(long lockTimeOut, TimeUnit unit);

    B errorListener(ChronicleHashErrorListener errorListener);

    B largeSegments(boolean largeSegments);

    B metaDataBytes(int metaDataBytes);

    B addReplicator(Replicator replicator);

    B setReplicators(Replicator... replicators);

    B timeProvider(TimeProvider timeProvider);

    B bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory);

    B objectSerializer(ObjectSerializer objectSerializer);

    B keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller);

    /**
     * Specifies that key objects, queried with the hash containers, created by this builder,
     * are inherently immutable. Keys in {@link ChronicleMap} or {@link ChronicleSet}
     * are not required to be immutable, as in ordinary {@link Map} or {@link Set} implementations,
     * because they are serialized off-heap. However, {@code ChronicleMap} and {@code ChronicleSet}
     * implementations can benefit from the knowledge that keys are not mutated between queries.
     *
     * <p>By default, {@code ChronicleHashBuilder}s detects immutability automatically
     * only for very few standard JDK types (for example, for {@link String}), it is not recommended
     * to rely on {@code ChronicleHashBuilder} to be smart enough about this.
     *
     * @return this builder back
     */
    B immutableKeys();

    C create(File file) throws IOException;

    C create() throws IOException;
}
