package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashErrorListener;
import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.hash.StatelessClientConfig;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
interface ChronicleMapBuilderI<K, V> extends Serializable {


    /**
     * {@inheritDoc} With respect to {@linkplain #entryAndValueAlignment(Alignment) alignment}.
     *
     * <p>Note that the actual entrySize will be aligned to 4 (default {@linkplain
     * #entryAndValueAlignment(Alignment) entry alignment}). I. e. if you set entry size to 30, and entry
     * alignment is set to {@link Alignment#OF_4_BYTES}, the actual entry size will be 32 (30 aligned to 4
     * bytes).
     *
     * @see #entryAndValueAlignment(Alignment) //  * @see #entries(long)
     */
    ChronicleMapBuilderI<K, V> entrySize(int entrySize);

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
     * <p>Default is {@link Alignment#OF_4_BYTES} for Byteable values.
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     */
    ChronicleMapBuilderI<K, V> entryAndValueAlignment(Alignment alignment);


    /**
     * {@inheritDoc} Also, it overrides any previous {@link #prepareValueBytesOnAcquire} configuration to this
     * {@code ChronicleMapBuilder}.
     *
     * <p>By default, the default value is not specified, default {@linkplain #prepareValueBytesOnAcquire
     * prepare value bytes routine} is specified instead.
     *
     * @see #defaultValueProvider(DefaultValueProvider)
     * @see #prepareValueBytesOnAcquire(PrepareValueBytes)
     */
    ChronicleMapBuilderI<K, V> defaultValue(V defaultValue);


    /**
     * Configures the procedure which is called on the bytes, which later the returned value is pointing to,
     * if the key is absent, on {@link ChronicleMap#acquireUsing(Object, Object) acquireUsing()} call on maps,
     * created by this builder. See {@link PrepareValueBytes} for more information.
     *
     * <p>The default preparation callback zeroes out the value bytes.
     *
     * @param prepareValueBytes what to do with the value bytes before assigning them into the {@link
     *                          net.openhft.lang.model.Byteable} value to return from {@code acquireUsing()}
     *                          call
     * @return this builder back
     * @see PrepareValueBytes
     * @see #defaultValue(Object)
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    ChronicleMapBuilderI<K, V> prepareValueBytesOnAcquire(
            @NotNull PrepareValueBytes<K, V> prepareValueBytes);


    /**
     * {@inheritDoc}
     *
     * @see #constantKeySizeBySample(Object)
     * @see #valueSize(int)
     * @see #entrySize(int)
     */
    ChronicleMapBuilderI<K, V> keySize(int keySize);

    /**
     * {@inheritDoc}
     *
     * @see #keySize(int)
     * @see #constantValueSizeBySample(Object)
     */

    ChronicleMapBuilderI<K, V> constantKeySizeBySample(K sampleKey);

    /**
     * Configures the optimal number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. If value size is always the same, call {@link #constantValueSizeBySample(Object)} method
     * instead of this one.
     *
     * <p>If value is a boxed primitive type, i. e. if value size is known statically, it is automatically
     * accounted and shouldn't be specified by user.
     *
     * <p>If value size varies moderately, specify the size higher than average, but lower than the maximum
     * possible, to minimize average memory overuse. If value size varies in a wide range, it's better to use
     * {@linkplain #entrySize(int) entry size} in "chunk" mode and configure it directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     * @see #constantValueSizeBySample(Object)
     * @see #keySize(int)
     * @see #entrySize(int)
     */

    ChronicleMapBuilderI<K, V> valueSize(int valueSize);

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. This is done by providing the {@code sampleValue}, all values should take the same number
     * of bytes in serialized form, as this sample object.
     *
     * <p>If values are of boxed primitive type or {@link net.openhft.lang.model.Byteable} subclass, i. e. if
     * value size is known statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If value size varies, method {@link #valueSize(int)} or {@link #entrySize(int)} should be called
     * instead of this one.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #valueSize(int)
     * @see #constantKeySizeBySample(Object)
     */


    ChronicleMapBuilderI<K, V> constantValueSizeBySample(
            @NotNull V sampleValue);

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
     */

    ChronicleMapBuilderI<K, V> objectSerializer(
            @NotNull ObjectSerializer objectSerializer);

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if custom value marshaller is specified or value class is not either
     *                               {@code BytesMarshallable} or {@code Externalizable}
     */

    ChronicleMapBuilderI<K, V> valueDeserializationFactory(
            @NotNull ObjectFactory<V> valueDeserializationFactory);

    /**
     * {@inheritDoc}
     *
     * <p>By default, default value provider is not specified, {@link #defaultValue(Object) default value} is
     * specified instead.
     *
     * @see #defaultValue(Object)
     */

    ChronicleMapBuilderI<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider);

    /**
     * Configures the {@code BytesMarshaller} used to serialize/deserialize values to/from off-heap memory in
     * maps, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
     *
     * @param valueMarshaller the marshaller used to serialize values
     * @return this builder back
     * @see #valueMarshallers(net.openhft.chronicle.hash.serialization.BytesWriter,
     * net.openhft.chronicle.hash.serialization.BytesReader)
     * @see ChronicleMapBuilder
     * @see #objectSerializer(ObjectSerializer)
     */
    ChronicleMapBuilderI<K, V> valueMarshaller(
            @NotNull BytesMarshaller<V> valueMarshaller);


    /**
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory in maps,
     * created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
     *
     * <p>Configuring marshalling this way results to a little bit more compact in-memory layout of the map,
     * comparing to a single interface configuration: {@link #valueMarshaller(BytesMarshaller)}.
     *
     * <p>Passing {@link net.openhft.chronicle.hash.serialization.BytesInterop} instead of plain {@link
     * net.openhft.chronicle.hash.serialization.BytesWriter} is, of cause, possible, but currently pointless
     * for values.
     *
     * @param valueWriter the new value object &rarr; {@link net.openhft.lang.io.Bytes} writer (interop)
     *                    strategy
     * @param valueReader the new {@link net.openhft.lang.io.Bytes} &rarr; value object reader strategy
     * @return this builder back
     * @see #valueMarshaller(BytesMarshaller)
     */
    ChronicleMapBuilderI<K, V> valueMarshallers(
            @NotNull BytesWriter<V> valueWriter, @NotNull BytesReader<V> valueReader);

    /**
     * Configures the marshaller used to serialize actual value sizes to off-heap memory in maps, created by
     * this builder.
     *
     * <p>Default value size marshaller is so-called {@linkplain net.openhft.chronicle.hash.serialization.SizeMarshallers#stopBit()
     * stop bit encoding marshalling}. If {@linkplain #constantValueSizeBySample(Object) constant value size}
     * is configured, or defaulted if the value type is always constant and {@code ChronicleHashBuilder}
     * implementation knows about it, this configuration takes no effect, because a special {@link
     * net.openhft.chronicle.hash.serialization.SizeMarshaller} implementation, which doesn't actually do any
     * marshalling, and just returns the known constant size on {@link net.openhft.chronicle.hash.serialization.SizeMarshaller#readSize(
     *net.openhft.lang.io.Bytes)} calls, is used instead of any {@code SizeMarshaller} configured using this
     * method.
     *
     * @param valueSizeMarshaller the new marshaller, used to serialize actual value sizes to off-heap memory
     * @return this builder back
     */
    ChronicleMapBuilderI<K, V> valueSizeMarshaller(
            @NotNull SizeMarshaller valueSizeMarshaller);

    ChronicleMapBuilderI<K, V> entries(long l);

    ChronicleMapBuilderI<K, V> replication(SingleChronicleHashReplication withId);

    ChronicleMap<K, V> create();

    ChronicleMapBuilderI<K, V> actualSegments(int actualSegments);

    ChronicleMapBuilderI<K, V> minSegments(int minSegments);

    ChronicleMapBuilderI<K, V> actualEntriesPerSegment(long actualEntriesPerSegment);

    ChronicleMapBuilderI<K, V> lockTimeOut(long lockTimeOut, TimeUnit unit);

    ChronicleMapBuilderI<K, V> errorListener(ChronicleHashErrorListener errorListener);

    ChronicleMapBuilderI<K, V> metaDataBytes(int metaDataBytes);

    ChronicleMapBuilderI<K, V> timeProvider(TimeProvider timeProvider);

    ChronicleMapBuilderI<K, V> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory);

    ChronicleMapBuilderI<K, V> keyMarshaller(BytesMarshaller<K> keyMarshaller);

    ChronicleMapBuilderI<K, V> keyMarshallers(BytesWriter<K> keyWriter, BytesReader<K> keyReader);

    ChronicleMapBuilderI<K, V> keySizeMarshaller(SizeMarshaller keySizeMarshaller);

    ChronicleMapBuilderI<K, V> keyDeserializationFactory(ObjectFactory<K> keyDeserializationFactory);

    ChronicleMapBuilderI<K, V> immutableKeys();

    ChronicleMap<K, V> createPersistedTo(File file) throws IOException;

    StatelessClientConfig<ChronicleMap<K, V>> statelessClient(InetSocketAddress remoteAddress);

    ChronicleHashInstanceConfig<ChronicleMap<K, V>> instance();

    ChronicleMapBuilderI<K, V> replication(byte identifier);

    ChronicleMapBuilderI<K, V> replication(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork);

    ChronicleMapBuilderI<K, V> clone();

    ChronicleMapBuilderI<K, V> eventListener(MapEventListener<K, V, ChronicleMap<K, V>> eventListener);

    ChronicleMapBuilderI<K, V> putReturnsNull(boolean b);

    ChronicleMapBuilderI<K, V> removeReturnsNull(boolean b);

    ChronicleMapBuilderI<K, V> name(String name);




    String name();


}
