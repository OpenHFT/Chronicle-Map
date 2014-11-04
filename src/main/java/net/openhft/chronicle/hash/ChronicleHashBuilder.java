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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.*;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.AllocateInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.NewInstanceObjectFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines the meaning of configurations, common to {@link ChronicleMapBuilder} and
 * {@link ChronicleSetBuilder}, i. e. <i>Chronicle hash container</i> configurations.
 *
 * <p>{@code ChronicleHashBuilder} is mutable. Configuration methods mutate the builder and return
 * <i>the builder itself</i> back to support chaining pattern, rather than the builder copies with
 * the corresponding configuration changed. To make an independent configuration, {@linkplain
 * #clone} the builder.
 *
 * @param <K> the type of keys in hash containers, created by this builder
 * @param <C> the container type, created by this builder, i. e. {@link ChronicleMap} or {@link
 *            ChronicleSet}
 * @param <B> the concrete builder type, i. e. {@link ChronicleMapBuilder} or {@link
 *            ChronicleSetBuilder}
 */
public interface ChronicleHashBuilder<K, C extends ChronicleHash,
        B extends ChronicleHashBuilder<K, C, B>> extends Cloneable {

    /**
     * Clones this builder. Useful for configuration persisting, because {@code
     * ChronicleHashBuilder}s are mutable and changed on each configuration method call. Original
     * and cloned builders are independent.
     *
     * @return a new clone of this builder
     */
    B clone();

    /**
     * Set minimum number of segments in hash containers, constructed by this builder. See
     * concurrencyLevel in {@link ConcurrentHashMap}.
     *
     * @param minSegments the minimum number of segments in containers, constructed by this builder
     * @return this builder object back
     */
    B minSegments(int minSegments);

    /**
     * Configures the optimal number of bytes, taken by serialized form of keys, put into hash
     * containers, created by this builder. If key size is always the same, call {@link
     * #constantKeySizeBySample(Object)} method instead of this one.
     *
     * <p>If key size varies moderately, specify the size higher than average, but lower than the
     * maximum possible, to minimize average memory overuse. If key size varies in a wide range,
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
     * containers, created by this builder. This is done by providing the {@code sampleKey}, all
     * keys should take the same number of bytes in serialized form, as this sample object.
     *
     * <p>If keys are of boxed primitive type or {@link Byteable} subclass, i. e. if key size is
     * known statically, it is automatically accounted and this method shouldn't be called.
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
     * Configures the size in bytes of allocation unit of hash container instances, created by this
     * builder.
     *
     * <p>{@link ChronicleMap} and {@link ChronicleSet} store their data off-heap, so it is required
     * to serialize key (and values, in {@code ChronicleMap} case) (unless they are direct {@link
     * Byteable} instances). Serialized key bytes (+ serialized value bytes, in {@code ChronicleMap}
     * case) + some metadata bytes comprise "entry space", which {@code ChronicleMap} or {@code
     * ChronicleSet} should allocate. So <i>entry size</i> is a minimum allocation portion in the
     * hash containers, created by this builder. E. g. if entry size is 100, the created container
     * could only allocate 100, 200, 300... bytes for an entry. If say 150 bytes of entry space are
     * required by the entry, 200 bytes will be allocated, 150 used and 50 wasted. To minimize
     * memory overuse and improve speed, you should pay decent attention to this configuration.
     *
     * <p>There are three major patterns of this configuration usage: <ol> <li>Key (and value, in
     * {@code ChronicleMap} case) sizes are constant. Configure them via {@link
     * #constantKeySizeBySample(Object)} and {@link ChronicleMapBuilder#constantValueSizeBySample(Object)}
     * methods, and you will experience no memory waste at all.</li> <li>Key (and/or value size, in
     * {@code ChronicleMap} case) varies moderately. Specify them using corresponding methods, or
     * specify entry size directly by calling this method, by sizes somewhere between average and
     * maximum possible. The idea is to have most (90% or more) entries to fit a single "entry size"
     * with moderate memory waste (10-20% on average), rest 10% or less of  entries should take 2
     * "entry sizes", thus with ~50% memory overuse.</li> <li>Key (and/or value size, in {@code
     * ChronicleMap} case) varies in a wide range. Then it's best to use entry size configuration in
     * <i>chunk mode</i>. Specify entry size so that most entries should take from 5 to several
     * dozens of "chunks". With this approach, average memory waste should be very low.
     *
     * <p>However, remember that <ul> <li>Operations with entries that span several "entry sizes"
     * are a bit slower, than entries which take a single "entry size". That is why "chunk" approach
     * is not recommended, when key (and/or value size) varies moderately.</li> <li>The maximum
     * number of chunks could be taken by an entry is 64. {@link IllegalArgumentException} is thrown
     * on attempt to insert too large entry, compared to the configured or computed entry size.</li>
     * <li>The number of "entries" {@linkplain #entries(long) in the whole map} and {@linkplain
     * #actualEntriesPerSegment(long) per segment} is actually the number "chunks".</li> </ul>
     *
     * <p>Example: if values in your {@code ChronicleMap} are adjacency lists of some social graph,
     * where nodes are represented as {@code long} ids, and adjacency lists are serialized in
     * efficient manner, for example as {@code long[]} arrays. Typical number of connections is
     * 100-300, maximum is 3000. In this case entry size of
     * 50 * (8 bytes for each id) = 400 bytes would be a good choice: <pre>{@code
     * Map<Long, long[]> socialGraph = ChronicleMapBuilder
     *     .of(Long.class, long[].class)
     *     // given that graph should have of 1 billion nodes, and 150 average adjacency list size
     *     // => values takes 3 chuncks on average
     *     .entries(1_000_000_000L * (150 / 50))
     *     .entrySize(50 * 8)
     *     .create();}</pre>
     * It is minimum possible (because 3000 friends / 50 friends = 60 is close to 64 "max chunks by
     * single entry" limit, and ensures moderate average memory overuse (not more than 20%). </li>
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
     * <ol> <li>If key size (and value size, in {@code ChronicleMap} case) is constant, this number
     * is equal to the maximum number of entries (because each entry takes exactly one "entry size"
     * memory unit).</li> <li>If key (and/or value, in {@code ChronicleMap} case) size varies
     * moderately, you should pass to this method the maximum number of entries + 5-25%, depending
     * on your data properties and configured {@linkplain #keySize(int) key}/{@linkplain
     * ChronicleMapBuilder#valueSize(int) value}/{@linkplain #entrySize(int) entry} sizes.</li>
     * <li>If your data size varies in a wide range, pass the maximum number of entries multiplied
     * by average data size and divided by the configured "entry size" (i. e. chunk size). See an
     * example in the documentation to {@link #entrySize(int)} method.</li> </ol>
     *
     * <p>You shouldn't put additional margin over the number, computed according the rules above.
     * This bad practice was popularized by {@link HashMap#HashMap(int)} and {@link
     * HashSet#HashSet(int)} constructors, which accept "capacity", that should be multiplied by
     * "load factor" to obtain actual maximum expected number of entries. {@code ChronicleMap} and
     * {@code ChronicleSet} don't have a notion of load factor.
     *
     * <p>Default value is 2^20 (~ 1 million).
     *
     * @param entries maximum size of the created maps, in memory allocation units, so-called "entry
     *                size"
     * @return this builder back
     * @see #entrySize(int)
     */
    B entries(long entries);

    B actualEntriesPerSegment(long actualEntriesPerSegment);

    B actualSegments(int actualSegments);

    /**
     * Configures timeout of locking on {@linkplain #actualSegments(int) segments} of hash
     * containers, created by this builder, when performing any queries, as well as bulk operations
     * like iteration. If timeout expires, {@link ChronicleHashErrorListener#onLockTimeout(long)} is
     * called, and then thread tries to obtain the segment lock one more time, and so in a loop,
     * until thread is interrupted. However, you can configure {@linkplain #errorListener(
     *ChronicleHashErrorListener) error listener} to throw an exception on the first (or n-th) lock
     * acquisition fail.
     *
     * <p>Default lock time out is 2 seconds.
     *
     * @param lockTimeOut new lock timeout for segments of containers created by this builder, in
     *                    the given time units
     * @param unit        time unit of the given lock timeout
     * @return this builder back
     */
    B lockTimeOut(long lockTimeOut, TimeUnit unit);

    B errorListener(ChronicleHashErrorListener errorListener);

    B metaDataBytes(int metaDataBytes);

    /**
     * Configures replication of the hash containers, created by this builder (via independent
     * replicators). See <a href="https://github.com/OpenHFT/Chronicle-Map#tcp--udp-replication">
     * the section about replication in ChronicleMap manual</a> for more information.
     *
     * <p>Another way to establish replication is {@link #channel(ChannelProvider.ChronicleChannel)}
     * method.
     *
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     *
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by this method or {@link #channel(ChannelProvider.ChronicleChannel)} method.
     *
     * @param identifier  the new {@linkplain Replica#identifier() identifier} of the containers,
     *                    created by this builder
     * @param replicators several replicators, used to sync the data across replicating nodes
     *                    independently
     * @return this builder back
     * @see #channel(ChannelProvider.ChronicleChannel)
     * @see #disableReplication()
     */
    B replicators(byte identifier, ReplicationConfig... replicators);

    /**
     * Configures replication of the hash containers, created by this builder, via so called
     * "channels". See <a href="https://github.com/OpenHFT/Chronicle-Map#channels-and-channelprovider">the
     * section about Channels and ChannelProvider in ChronicleMap manual</a> for more information.
     *
     * <p>Another way to establish replication if {@link #replicators(byte, ReplicationConfig[])}
     * method.
     *
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     *
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by means of this method or {@link #replicators(byte, ReplicationConfig[])} method
     * calls.
     *
     * @param chronicleChannel the channel responsible for gathering updates of hash containers,
     *                         created by this builder, and replicating them over network
     * @return this builder object back
     * @see #replicators(byte, ReplicationConfig[])
     * @see #disableReplication()
     */
    B channel(ChannelProvider.ChronicleChannel chronicleChannel);

    /**
     * Specifies that hash containers, created by this builder, shouldn't replicate their data over
     * network. This method call clears any replication configurations, made via {@link
     * #replicators(byte, ReplicationConfig[])} or {@link #channel(ChannelProvider.ChronicleChannel)}
     * method call.
     *
     * <p>By default replication is not configured, so this method is useful <i>only</i> to ensure
     * that hash containers, created by some inherited or passed builder, wouldn't replicate. I. e.
     * in rare cases.
     *
     * @return this builder back
     * @see #replicators(byte, ReplicationConfig[])
     * @see #channel(ChannelProvider.ChronicleChannel)
     */
    B disableReplication();

    /**
     * Configures a time provider, used by hash containers, created by this builder, for needs of
     * replication consensus protocol (conflicting data updates resolution).
     *
     * <p>Default time provider is {@link TimeProvider#SYSTEM}.
     *
     * @param timeProvider a new time provider for replication needs
     * @return this builder back
     * @see #replicators(byte, ReplicationConfig[])
     * @see #channel(ChannelProvider.ChronicleChannel)
     */
    B timeProvider(TimeProvider timeProvider);

    /**
     * Configures a {@link BytesMarshallerFactory} to be used with {@link
     * BytesMarshallableSerializer}, which is a default {@link #objectSerializer ObjectSerializer},
     * to serialize/deserialize data to/from off-heap memory in hash containers, created by this
     * builder.
     *
     * <p>Default {@code BytesMarshallerFactory} is an instance of {@link
     * VanillaBytesMarshallerFactory}. This is a convenience configuration method, it has no effect
     * on the resulting hash containers, if {@linkplain #keyMarshaller(BytesMarshaller) custom data
     * marshallers} are configured, data types extends one of specific serialization interfaces,
     * recognized by this builder (e. g. {@code Externalizable} or {@code BytesMarshallable}), or
     * {@code ObjectSerializer} is configured.
     *
     * @param bytesMarshallerFactory the marshaller factory to be used with the default {@code
     *                               ObjectSerializer}, i. e. {@code BytesMarshallableSerializer}
     * @return this builder back
     * @see #objectSerializer(ObjectSerializer)
     */
    B bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory);

    /**
     * Configures the serializer used to serialize/deserialize data to/from off-heap memory, when
     * specified class doesn't implement a specific serialization interface like {@link
     * Externalizable} or {@link BytesMarshallable} (for example, if data is loosely typed and just
     * {@code Object} is specified as the data class), or nullable data, and if custom marshaller is
     * not {@linkplain #keyMarshaller(BytesMarshaller) configured}, in hash containers, created by
     * this builder. Please read {@link ObjectSerializer} docs for more info and available options.
     *
     * <p>Default serializer is {@link BytesMarshallableSerializer}, configured with the specified
     * or default {@link #bytesMarshallerFactory(BytesMarshallerFactory) BytesMarshallerFactory}.
     *
     * @param objectSerializer the serializer used to serialize loosely typed or nullable data if
     *                         custom marshaller is not configured
     * @return this builder back
     * @see #bytesMarshallerFactory(BytesMarshallerFactory)
     * @see #keyMarshaller(BytesMarshaller)
     */
    B objectSerializer(ObjectSerializer objectSerializer);

    /**
     * Configures the {@code BytesMarshaller} used to serialize/deserialize keys to/from off-heap
     * memory in hash containers, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
     *
     * @param keyMarshaller the marshaller used to serialize keys
     * @return this builder back
     * @see #objectSerializer(ObjectSerializer)
     */
    B keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller);

    /**
     * Configures factory which is used to create a new key instance, if key class is either
     * {@link Byteable}, {@link BytesMarshallable} or {@link Externalizable} subclass in maps,
     * created by this builder. If {@linkplain #keyMarshaller(BytesMarshaller) custom key
     * marshaller} is configured, this configuration is unused, because it is incapsulated in
     * {@link BytesMarshaller#read(Bytes)} method (without provided instance to read the data into),
     * i. e. it's is the user-side responsibility.
     *
     * <p>Default key deserialization factory is {@link NewInstanceObjectFactory}, which creates
     * a new key instance using {@link Class#newInstance()} default constructor. You could provide
     * an {@link AllocateInstanceObjectFactory}, which uses {@code Unsafe.allocateInstance(Class)}
     * (you might want to do this for better performance or if you don't want to initialize fields),
     * or a factory which calls a key class constructor with some arguments, or a factory which
     * internally delegates to instance pool or {@link ThreadLocal}, to reduce allocations.
     *
     * @param keyDeserializationFactory the key factory used to produce instances to deserialize
     *                                  data in
     * @return this builder back
     * @throws IllegalStateException if custom key marshaller is specified or key class is not
     *         either {@code Byteable}, {@code BytesMarshallable} or {@code Externalizable}
     */
    B keyDeserializationFactory(@NotNull ObjectFactory<K> keyDeserializationFactory);

    /**
     * Specifies that key objects, queried with the hash containers, created by this builder, are
     * inherently immutable. Keys in {@link ChronicleMap} or {@link ChronicleSet} are not required
     * to be immutable, as in ordinary {@link Map} or {@link Set} implementations, because they are
     * serialized off-heap. However, {@code ChronicleMap} and {@code ChronicleSet} implementations
     * can benefit from the knowledge that keys are not mutated between queries.
     *
     * <p>By default, {@code ChronicleHashBuilder}s detects immutability automatically only for very
     * few standard JDK types (for example, for {@link String}), it is not recommended to rely on
     * {@code ChronicleHashBuilder} to be smart enough about this.
     *
     * @return this builder back
     */
    B immutableKeys();

    /**
     * Specifies that hash containers, created by this builder, should be a stateless
     * implementation, this stateless implementation will be referred to as a stateless client as it
     * will not hold any of its own data locally, the stateless client will perform Remote Procedure
     * Calls ( RPC ) to another {@link ChronicleMap} or {@link ChronicleSet} which we will refer to
     * as the server. The server will hold all your data, the server can not it’s self be a
     * stateless client. Your stateless client must be connected to the server via TCP/IP. The
     * stateless client will delegate all your method calls to the remote sever. The stateless
     * client operations will block, in other words the stateless client will wait for the server to
     * send a response before continuing to the next operation. The stateless client could be
     * consider to be a ClientProxy to  {@link ChronicleMap} or {@link ChronicleSet}  running on
     * another host
     *
     * @param statelessBuilder Stateless Client Config, {@code null} demotes no statelessBuilder
     * @return this builder back
     */
    B stateless(StatelessBuilder statelessBuilder);


    /**
     * Specifies that hash containers us the specified file, if create is called and this is not set
     * the map will be created in off-heap memory, and changes will not be persisted to disk. If a
     * file is provided all  changes to the map are  persisted to disk (this is an operating system
     * guarantee) independently from JVM process lifecycle.
     *
     * @param file the file with existing hash container or a desired location of a new off-heap
     *             persisted hash container
     * @return this builder back
     * @throws IOException If the file cannot be created and read.
     * @see ChronicleHash#file()
     * @see ChronicleHash#close()
     */
    B file(File file) throws IOException;

    /**
     * Creates a new hash container, storing it's data in off-heap memory, not mapped to any file.
     * On {@link ChronicleHash#close()} called on the returned container, or after the container
     * object is collected during GC, or on JVM shutdown the off-heap memory used by the returned
     * container is freed.
     *
     * @return a new off-heap hash container
     * @throws IOException if any IO error relates to off-heap memory allocation occurs
     * @see #create()
     */
    C create() throws IOException;






}
