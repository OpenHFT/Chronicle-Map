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

import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.map.ChronicleMap;
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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines the meaning of configurations, common to {@link
 * net.openhft.chronicle.map.OnHeapUpdatableChronicleMapBuilder} and {@link ChronicleSetBuilder}, i.
 * e. <i>Chronicle hash container</i> configurations.
 *
 * <p>{@code ChronicleHashBuilder} is mutable. Configuration methods mutate the builder and return
 * <i>the builder itself</i> back to support chaining pattern, rather than the builder copies with
 * the corresponding configuration changed. To make an independent configuration, {@linkplain
 * #clone} the builder.
 *
 * @param <K> the type of keys in hash containers, created by this builder
 * @param <C> the container type, created by this builder, i. e. {@link ChronicleMap} or {@link
 *            ChronicleSet}
 * @param <B> the concrete builder type, i. e. {@link net.openhft.chronicle.map.ChronicleMapBuilder}
 *            or {@link ChronicleSetBuilder}
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
     * #constantKeySizeBySample(Object)} and {@link net.openhft.chronicle.map.OnHeapUpdatableChronicleMapBuilder#constantValueSizeBySample(Object)}
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
     * Map<Long, long[]> socialGraph = ChronicleMapOnHeapUpdatableBuilder
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
     * net.openhft.chronicle.map.OnHeapUpdatableChronicleMapBuilder#valueSize(int)
     * value}/{@linkplain #entrySize(int) entry} sizes.</li> <li>If your data size varies in a wide
     * range, pass the maximum number of entries multiplied by average data size and divided by the
     * configured "entry size" (i. e. chunk size). See an example in the documentation to {@link
     * #entrySize(int)} method.</li> </ol>
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
     * until thread is interrupted. However, you can configure {@linkplain
     * #errorListener(ChronicleHashErrorListener) error listener} to throw an exception on the first
     * (or n-th) lock acquisition fail.
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
     * Configures a time provider, used by hash containers, created by this builder, for needs of
     * replication consensus protocol (conflicting data updates resolution).
     *
     * <p>Default time provider is {@link TimeProvider#SYSTEM}.
     *
     * @param timeProvider a new time provider for replication needs
     * @return this builder back
     * @see #replication(SingleChronicleHashReplication)
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
     * @see #keyMarshallers(BytesWriter, BytesReader)
     * @see #objectSerializer(ObjectSerializer)
     */
    B keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller);

    /**
     * Configures the marshallers, used to serialize/deserialize keys to/from off-heap memory in
     * hash containers, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
     *
     * <p>Configuring marshalling this way results to a little bit more compact in-memory layout of
     * the map, comparing to a single interface configuration: {@link #keyMarshaller(BytesMarshaller)}.
     *
     * <p>Passing {@link BytesInterop} (which is a subinterface of {@link BytesWriter}) as the first
     * argument is supported, and even more advantageous from performance perspective.
     *
     * @param keyWriter the new key object &rarr; {@link Bytes} writer (interop) strategy
     * @param keyReader the new {@link Bytes} &rarr; key object reader strategy
     * @return this builder back
     * @see #keyMarshaller(BytesMarshaller)
     */
    B keyMarshallers(@NotNull BytesWriter<K> keyWriter, @NotNull BytesReader<K> keyReader);

    /**
     * Configures the marshaller used to serialize actual key sizes to off-heap memory in hash
     * containers, created by this builder.
     *
     * <p>Default key size marshaller is so-called {@linkplain SizeMarshallers#stopBit() stop bit
     * encoding marshalling}. If {@linkplain #constantKeySizeBySample(Object) constant key size} is
     * configured, or defaulted if the key type is always constant and {@code ChronicleHashBuilder}
     * implementation knows about it, this configuration takes no effect, because a special {@link
     * SizeMarshaller} implementation, which doesn't actually do any marshalling, and just returns
     * the known constant size on {@link SizeMarshaller#readSize(Bytes)} calls, is used instead of
     * any {@code SizeMarshaller} configured using this method.
     *
     * @param keySizeMarshaller the new marshaller, used to serialize actual key sizes to off-heap
     *                          memory
     * @return this builder back
     */
    B keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller);

    /**
     * Configures factory which is used to create a new key instance, if key class is either {@link
     * Byteable}, {@link BytesMarshallable} or {@link Externalizable} subclass in maps, created by
     * this builder. If {@linkplain #keyMarshaller(BytesMarshaller) custom key marshaller} is
     * configured, this configuration is unused, because it is incapsulated in {@link
     * BytesMarshaller#read(Bytes)} method (without provided instance to read the data into), i. e.
     * it's is the user-side responsibility.
     *
     * <p>Default key deserialization factory is {@link NewInstanceObjectFactory}, which creates a
     * new key instance using {@link Class#newInstance()} default constructor. You could provide an
     * {@link AllocateInstanceObjectFactory}, which uses {@code Unsafe.allocateInstance(Class)} (you
     * might want to do this for better performance or if you don't want to initialize fields), or a
     * factory which calls a key class constructor with some arguments, or a factory which
     * internally delegates to instance pool or {@link ThreadLocal}, to reduce allocations.
     *
     * @param keyDeserializationFactory the key factory used to produce instances to deserialize
     *                                  data in
     * @return this builder back
     * @throws IllegalStateException if custom key marshaller is specified or key class is not
     *                               either {@code Byteable}, {@code BytesMarshallable} or {@code
     *                               Externalizable}
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
     * Configures replication of the hash containers, created by this builder. See <a
     * href="https://github.com/OpenHFT/Chronicle-Map#tcp--udp-replication"> the section about
     * replication in ChronicleMap manual</a> for more information.
     *
     * <p>By default, hash containers, created by this builder doesn't replicate their data.
     *
     * <p>This method call overrides all previous replication configurations of this builder, made
     * either by this method or {@link #replication(byte, TcpTransportAndNetworkConfig)} shortcut
     * method.
     *
     * @param replication the replication config
     * @return this builder back
     * @see ChronicleHashInstanceConfig#replicated(SingleChronicleHashReplication)
     * @see #replication(byte, TcpTransportAndNetworkConfig)
     */
    B replication(SingleChronicleHashReplication replication);

    /**
     * Shortcut for {@code replication(SimpleReplication.builder() .tcpTransportAndNetwork(tcpTransportAndNetwork).createWithId(identifier))}.
     *
     * @param identifier             the network-wide identifier of the containers, created by this
     *                               builder
     * @param tcpTransportAndNetwork configuration of tcp connection and network
     * @return this builder back
     * @see #replication(SingleChronicleHashReplication)
     * @see ChronicleHashInstanceConfig#replicated(byte, TcpTransportAndNetworkConfig)
     */
    B replication(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork);

    B replication(byte identifier);

    StatelessClientConfig<C> statelessClient(InetSocketAddress remoteAddress);

    ChronicleHashInstanceConfig<C> instance();

    /**
     * Creates a new hash container, storing it's data in off-heap memory, not mapped to any file.
     * On {@link ChronicleHash#close()} called on the returned container, or after the container
     * object is collected during GC, or on JVM shutdown the off-heap memory used by the returned
     * container is freed.
     *
     * <p>This method is a shortcut for {@code instance().create()}.
     *
     * @return a new off-heap hash container
     * @throws IOException if any IO error relates to off-heap memory allocation, or establishing
     *                     replication connections, occurs
     * @see #createPersistedTo(File)
     * @see #instance()
     */
    C create() throws IOException;

    /**
     * Opens a hash container residing the specified file, or creates a new one if the file not yet
     * exists and maps its off-heap memory to the file. All changes to the map are persisted to disk
     * (this is an operating system guarantee) independently from JVM process lifecycle.
     *
     * <p>Multiple containers could give access to the same data simultaneously, either inside a
     * single JVM or across processes. Access is synchronized correctly across all instances, i. e.
     * hash container mapping the data from the first JVM isn't able to modify the data,
     * concurrently accessed from the second JVM by another hash container instance, mapping the
     * same data.
     *
     * <p>On container's {@link ChronicleHash#close() close()} the data isn't removed, it remains on
     * disk and available to be opened again (given the same file name) or during different JVM
     * run.
     *
     * <p>This method is shortcut for {@code instance().persistedTo(file).create()}.
     *
     * @param file the file with existing hash container or a desired location of a new off-heap
     *             persisted hash container
     * @return a hash container mapped to the given file
     * @throws IOException if any IO error, related to off-heap memory allocation or file mapping,
     *                     or establishing replication connections, occurs
     * @see ChronicleHash#file()
     * @see ChronicleHash#close()
     * @see #create()
     * @see ChronicleHashInstanceConfig#persistedTo(File)
     */
    C createPersistedTo(File file) throws IOException;


    String name();
}
