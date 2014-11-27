package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
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
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * {@code ChronicleMapBuilder} is intended to be used to create {@link ChronicleMap}s
 *
 * @param <K> key type of the maps, created by this builder
 * @param <V> value type of the maps, created by this builder
 * @see AbstractChronicleMapBuilder
 * @see net.openhft.chronicle.hash.ChronicleHashBuilder
 */
public final class ChronicleMapBuilder<K, V> implements ChronicleMapBuilderI<K, V>, ChronicleHashBuilder<K,
        ChronicleMap<K, V>, ChronicleMapBuilder<K, V>> {

    final ChronicleMapBuilderI<K, V> delegate;

    /**
     * Returns a new {@code ChronicleMapBuilder} instance which is able to {@linkplain #create() create} maps
     * with the specified key and value classes.
     *
     * <p>{@code ChronicleMapBuilder} analyzes provided key and value classes and automatically chooses the
     * most specific internal builder.
     *
     * @param keyClass   class object used to infer key type and discover it's properties via reflection
     * @param valueClass class object used to infer value type and discover it's properties via reflection
     * @param <K>        key type of the maps, created by the returned builder
     * @param <V>        value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {

        if (valueClass.isEnum())
            return new ChronicleMapBuilder<K, V>(OnHeapUpdatableChronicleMapBuilder.of(keyClass, valueClass));

        if (keyClass.isInterface() && !builtInType(keyClass))
            keyClass = DataValueClasses.directClassFor(keyClass);

        if ((valueClass.isInterface() && !builtInType(valueClass))) {
            valueClass = DataValueClasses.directClassFor(valueClass);
        } else if (!offHeapReference(valueClass)) {
            return new ChronicleMapBuilder<K, V>(OnHeapUpdatableChronicleMapBuilder.of(keyClass, valueClass));
        }

        ChronicleMapBuilderI<K, V> builder = new OffHeapUpdatableChronicleMapBuilder<K, V>(keyClass, valueClass);
        return new ChronicleMapBuilder<K, V>(builder);
    }

    static boolean builtInType(Class clazz) {
        return clazz.getClassLoader() == Class.class.getClassLoader();
    }

    ChronicleMapBuilder(ChronicleMapBuilderI<K, V> delegate) {
        this.delegate = delegate;
    }


    private static boolean offHeapReference(Class valueClass) {
        return Byteable.class.isAssignableFrom(valueClass);
    }


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
    @Override
    public ChronicleMapBuilder<K, V> entrySize(int entrySize) {
        delegate.entrySize(entrySize);
        return this;
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
     * <p>Default is {@link Alignment#OF_4_BYTES} for Byteable values.
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     */
    @Override
    public ChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        delegate.entryAndValueAlignment(alignment);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        delegate.defaultValue(defaultValue);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> prepareValueBytesOnAcquire(@NotNull PrepareValueBytes<K, V> prepareValueBytes) {
        delegate.prepareValueBytesOnAcquire(prepareValueBytes);
        return this;
    }


    @Override
    public ChronicleMapBuilder<K, V> keySize(int keySize) {
        delegate.keySize(keySize);
        return this;
    }


    @Override
    public ChronicleMapBuilder<K, V> constantKeySizeBySample(K sampleKey) {
        delegate.constantKeySizeBySample(sampleKey);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> valueSize(int valueSize) {
        delegate.valueSize(valueSize);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> constantValueSizeBySample(@NotNull V sampleValue) {
        delegate.constantValueSizeBySample(sampleValue);
        return this;
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
     */
    @Override
    public ChronicleMapBuilder<K, V> objectSerializer(@NotNull ObjectSerializer objectSerializer) {
        delegate.objectSerializer(objectSerializer);
        return this;
    }


    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if custom value marshaller is specified or value class is not either
     *                               {@code BytesMarshallable} or {@code Externalizable}
     */

    @Override
    public ChronicleMapBuilder<K, V> valueDeserializationFactory(@NotNull ObjectFactory<V> valueDeserializationFactory) {
        delegate.valueDeserializationFactory(valueDeserializationFactory);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>By default, default value provider is not specified, {@link #defaultValue(Object) default value} is
     * specified instead.
     *
     * @see #defaultValue(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> defaultValueProvider(@NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        delegate.defaultValueProvider(defaultValueProvider);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> valueMarshaller(
            @NotNull BytesMarshaller<V> valueMarshaller) {
        delegate.valueMarshaller(valueMarshaller);
        return this;
    }

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
    @Override
    public ChronicleMapBuilder<K, V> valueMarshallers(@NotNull BytesWriter<V> valueWriter, @NotNull BytesReader<V> valueReader) {
        delegate.valueMarshallers(valueWriter, valueReader);
        return this;
    }


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
    @Override
    public ChronicleMapBuilder<K, V> valueSizeMarshaller(@NotNull SizeMarshaller valueSizeMarshaller) {
        delegate.valueSizeMarshaller(valueSizeMarshaller);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> entries(long l) {
        delegate.entries(l);
        return this;
    }


    /**
     * each map can be identified by a name
     *
     * @param name the name of the map
     * @return self
     */
    @Override
    public ChronicleMapBuilder<K, V> name(String name) {
        delegate.name(name);
        return this;
    }


    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public ChronicleMapBuilder<K, V> replication(SingleChronicleHashReplication withId) {
        delegate.replication(withId);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> pushTo(InetSocketAddress... addresses) {
        delegate.pushTo(addresses);
        return this;
    }

    @Override
    public ChronicleMap<K, V> create() {
        return delegate.create();

    }

    @Override
    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        delegate.actualSegments(actualSegments);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> minSegments(int minSegments) {
        delegate.minSegments(minSegments);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> actualEntriesPerSegment(long actualEntriesPerSegment) {
        delegate.actualEntriesPerSegment(actualEntriesPerSegment);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        delegate.lockTimeOut(lockTimeOut, unit);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> errorListener(ChronicleHashErrorListener errorListener) {
        delegate.errorListener(errorListener);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> metaDataBytes(int metaDataBytes) {
        delegate.metaDataBytes(metaDataBytes);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> timeProvider(TimeProvider timeProvider) {
        delegate.timeProvider(timeProvider);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        delegate.bytesMarshallerFactory(bytesMarshallerFactory);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshaller(BytesMarshaller<K> keyMarshaller) {
        delegate.keyMarshaller(keyMarshaller);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyMarshallers(BytesWriter<K> keyWriter, BytesReader<K> keyReader) {
        delegate.keyMarshallers(keyWriter, keyReader);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keySizeMarshaller(SizeMarshaller keySizeMarshaller) {
        delegate.keySizeMarshaller(keySizeMarshaller);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> keyDeserializationFactory(ObjectFactory<K> keyDeserializationFactory) {
        delegate.keyDeserializationFactory(keyDeserializationFactory);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> immutableKeys() {
        delegate.immutableKeys();
        return this;
    }


    @Override
    public StatelessClientConfig<ChronicleMap<K, V>> statelessClient(InetSocketAddress remoteAddress) {
        return delegate.statelessClient(remoteAddress);
    }

    @Override
    public ChronicleHashInstanceConfig<ChronicleMap<K, V>> instance() {
        return delegate.instance();
    }

    @Override
    public ChronicleMapBuilder<K, V> replication(byte identifier) {
        delegate.replication(identifier);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> replication(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        delegate.replication(identifier, tcpTransportAndNetwork);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> clone() {
        delegate.clone();
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> putReturnsNull(boolean b) {
        delegate.putReturnsNull(b);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> removeReturnsNull(boolean b) {
        delegate.removeReturnsNull(b);
        return this;
    }

    @Override
    public ChronicleMapBuilder<K, V> eventListener(MapEventListener<K, V> eventListener) {
        delegate.eventListener(eventListener);
        return this;
    }

    @Override
    public ChronicleMapBuilderI<K, V> bytesEventListener(BytesMapEventListener eventListener) {
        delegate.bytesEventListener(eventListener);
        return this;
    }


    @Override
    public ChronicleMap<K, V> createPersistedTo(File file) throws IOException {
        return delegate.createPersistedTo(file);
    }

    /**
     * @param bootstrapOnlyLocalEntries if set to true - when a new node joins a TCP replication grid, the new
     *                                  node will be populated with data, only for the nodes that created that
     *                                  data. Otherwise, all the nodes will publish all the data they have (
     *                                  potentially swamping the new node with duplicates ) however this does
     *                                  guarantee that all the data is replicated over to the new node, and is
     *                                  useful especially in the case that the originating node is not
     *                                  currently running.
     */
    public ChronicleMapBuilder<K, V> bootstrapOnlyLocalEntries(boolean bootstrapOnlyLocalEntries) {
        delegate.bootstrapOnlyLocalEntries(bootstrapOnlyLocalEntries);
        return this;
    }

}
