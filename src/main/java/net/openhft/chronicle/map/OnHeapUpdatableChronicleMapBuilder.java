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

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.Serializable;

/**
 * {@code ChronicleMapOnHeapUpdatableBuilder} is intended to be used to configure {@link ChronicleMap}s with
 * non-{@link Byteable} values, which don't point to off-heap memory directly, including primitives ({@link
 * Long}, {@link Double}, etc.), {@link String}s and {@link CharSequence}s, values implementing {@link
 * BytesMarshallable}, {@link Externalizable} or {@link Serializable} interface, or any other values for which
 * {@linkplain #valueMarshaller(BytesMarshaller) custom marshaller} is provided. <p/> <p>Use static {@link
 * #of(Class, Class) of(Key.class, Value.class)} method to obtain a {@code ChronicleMapOnHeapUpdatableBuilder}
 * instance.
 *
 * @param <K> key type of the maps, created by this builder
 * @param <V> value type of the maps, created by this builder
 * @see ChronicleMapBuilder
 * @see AbstractChronicleMapBuilder
 * @see ChronicleHashBuilder
 */
final class OnHeapUpdatableChronicleMapBuilder<K, V>
        extends AbstractChronicleMapBuilder<K, V, OnHeapUpdatableChronicleMapBuilder<K, V>> {

    /**
     * Returns a new {@code ChronicleMapOnHeapUpdatableBuilder} instance which is able to {@linkplain
     * #create() create} maps with the specified key and value classes. <p/> <p>{@code
     * ChronicleMapOnHeapUpdatableBuilder} analyzes provided key and value classes and automatically chooses
     * the most specific and effective serializer which it is aware about. Read <a
     * href="https://github.com/OpenHFT/Chronicle-Map#serialization">the section about serialization in
     * Chronicle Map manual</a> for more information.
     *
     * @param keyClass   class object used to infer key type and discover it's properties via reflection
     * @param valueClass class object used to infer value type and discover it's properties via reflection
     * @param <K>        key type of the maps, created by the returned builder
     * @param <V>        value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilderI<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
        return new OnHeapUpdatableChronicleMapBuilder<K, V>(keyClass, valueClass);
    }

    OnHeapUpdatableChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        super(keyClass, valueClass);
    }

    @Override
    OnHeapUpdatableChronicleMapBuilder<K, V> self() {
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @see #constantKeySizeBySample(Object)
     * @see #valueSize(int)
     * @see #entrySize(int)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> keySize(int keySize) {
        return super.keySize(keySize);
    }

    /**
     * {@inheritDoc}
     *
     * @see #keySize(int)
     * @see #constantValueSizeBySample(Object)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> constantKeySizeBySample(K sampleKey) {
        return super.constantKeySizeBySample(sampleKey);
    }

    /**
     * Configures the optimal number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. If value size is always the same, call {@link #constantValueSizeBySample(Object)} method
     * instead of this one.  If value is a boxed primitive type, i. e. if value size is known
     * statically, it is automatically accounted and shouldn't be specified by user. <p/> If value size
     * varies moderately, specify the size higher than average, but lower than the maximum possible, to
     * minimize average memory overuse. If value size varies in a wide range, it's better to use {@linkplain
     * #entrySize(int) entry size} in "chunk" mode and configure it directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this {@code ChronicleMapOnHeapUpdatableBuilder} back
     * @see #constantValueSizeBySample(Object)
     * @see #keySize(int)
     * @see #entrySize(int)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> valueSize(int valueSize) {
        return super.valueSize(valueSize);
    }

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps, created by
     * this builder. This is done by providing the {@code sampleValue}, all values should take the same number
     * of bytes in serialized form, as this sample object.  If values are of boxed primitive type or
     * {@link Byteable} subclass, i. e. if value size is known statically, it is automatically accounted and
     * this method shouldn't be called. If value size varies, method {@link #valueSize(int)} or {@link
     * #entrySize(int)} should be called instead of this one.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #valueSize(int)
     * @see #constantKeySizeBySample(Object)
     */

    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> constantValueSizeBySample(
            @NotNull V sampleValue) {
        return super.constantValueSizeBySample(sampleValue);
    }

    /**
     * {@inheritDoc} <p/>
     * <p>Example: <pre>{@code Map<Key, Value> map =
     *     ChronicleMapBuilder.of(Key.class, Value.class)
     *     .entries(1_000_000)
     *     .keySize(50).valueSize(200)
     *     // this class hasn't implemented yet, just for example
     *     .objectSerializer(new KryoObjectSerializer())
     *     .create();}</pre>
     * <p/> <p>This serializer is used to serialize both keys and values, if they both require this: loosely
     * typed, nullable, and custom {@linkplain #keyMarshaller(BytesMarshaller) key} and {@linkplain
     * #valueMarshaller(BytesMarshaller) value} marshallers are not configured.
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> objectSerializer(
            @NotNull ObjectSerializer objectSerializer) {
        return super.objectSerializer(objectSerializer);
    }

    /**
     * {@inheritDoc} <p/> <p>If {@linkplain #valueMarshaller(BytesMarshaller) custom value marshaller} is
     * configured, this configuration is unused, because it is incapsulated in {@link
     * BytesMarshaller#read(Bytes)} method (without provided instance to read the data into), i. e. it's is
     * the user-side responsibility. Actually this is just a convenience method supporting value marshaller
     * configurations, made initially during {@link #of(Class, Class)} call, if the value class is {@link
     * BytesMarshallable} or {@link Externalizable} subclass.
     *
     * @throws IllegalStateException if custom value marshaller is specified or value class is not either
     *                               {@code BytesMarshallable} or {@code Externalizable}
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> valueDeserializationFactory(
            @NotNull ObjectFactory<V> valueDeserializationFactory) {
        return super.valueDeserializationFactory(valueDeserializationFactory);
    }

    /**
     * {@inheritDoc} <p/> <p>By default, the default value is set to {@code null}.
     *
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        return super.defaultValue(defaultValue);
    }

    /**
     * {@inheritDoc} <p/> <p>By default, default value provider is not specified, {@link #defaultValue(Object)
     * default value} is specified instead.
     *
     * @see #defaultValue(Object)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        return super.defaultValueProvider(defaultValueProvider);
    }

    /**
     * Configures the {@code BytesMarshaller} used to serialize/deserialize values to/from off-heap memory in
     * maps, created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information.
     *
     * @param valueMarshaller the marshaller used to serialize values
     * @return this builder back
     * @see #valueMarshallers(BytesWriter, BytesReader)
     * @see ChronicleMapBuilder
     * @see #objectSerializer(ObjectSerializer)
     */
    public OnHeapUpdatableChronicleMapBuilder<K, V> valueMarshaller(
            @NotNull BytesMarshaller<V> valueMarshaller) {
        valueBuilder.marshaller(valueMarshaller);
        return this;
    }

    /**
     * Configures the marshallers, used to serialize/deserialize values to/from off-heap memory in maps,
     * created by this builder. See <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the
     * section about serialization in ChronicleMap manual</a> for more information. <p/> <p>Configuring
     * marshalling this way results to a little bit more compact in-memory layout of the map, comparing to a
     * single interface configuration: {@link #valueMarshaller(BytesMarshaller)}. <p/> <p>Passing {@link
     * net.openhft.chronicle.hash.serialization.BytesInterop} instead of plain {@link BytesWriter} is, of
     * cause, possible, but currently pointless for values.
     *
     * @param valueWriter the new value object &rarr; {@link Bytes} writer (interop) strategy
     * @param valueReader the new {@link Bytes} &rarr; value object reader strategy
     * @return this builder back
     * @see #valueMarshaller(BytesMarshaller)
     */
    public OnHeapUpdatableChronicleMapBuilder<K, V> valueMarshallers(
            @NotNull BytesWriter<V> valueWriter, @NotNull BytesReader<V> valueReader) {
        valueBuilder.writer(valueWriter);
        valueBuilder.reader(valueReader);
        return this;
    }

    /**
     * Configures the marshaller used to serialize actual value sizes to off-heap memory in maps, created by
     * this builder. <p/> <p>Default value size marshaller is so-called {@linkplain
     * net.openhft.chronicle.hash.serialization.SizeMarshallers#stopBit() stop bit encoding marshalling}. If
     * {@linkplain #constantValueSizeBySample(Object) constant value size} is configured, or defaulted if the
     * value type is always constant and {@code ChronicleHashBuilder} implementation knows about it, this
     * configuration takes no effect, because a special {@link SizeMarshaller} implementation, which doesn't
     * actually do any marshalling, and just returns the known constant size on {@link
     * SizeMarshaller#readSize(Bytes)} calls, is used instead of any {@code SizeMarshaller} configured using
     * this method.
     *
     * @param valueSizeMarshaller the new marshaller, used to serialize actual value sizes to off-heap memory
     * @return this builder back
     */
    public OnHeapUpdatableChronicleMapBuilder<K, V> valueSizeMarshaller(
            @NotNull SizeMarshaller valueSizeMarshaller) {
        valueBuilder.sizeMarshaller(valueSizeMarshaller);
        return this;
    }

    /**
     * Configures the procedure which is called on the bytes, which later the returned value is pointing to,
     * if the key is absent, on {@link ChronicleMap#acquireUsing(Object, Object) acquireUsing()} call on maps,
     * created by this builder. See {@link PrepareValueBytes} for more information. <p/> <p>The default
     * preparation callback zeroes out the value bytes.
     *
     * @param prepareValueBytes what to do with the value bytes before assigning them into the {@link
     *                          Byteable} value to return from {@code acquireUsing()} call
     * @return this builder back
     * @see PrepareValueBytes
     * @see #defaultValue(Object)
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    @Override
    public OnHeapUpdatableChronicleMapBuilder<K, V> prepareValueBytesOnAcquire(
            @NotNull PrepareValueBytes<K, V> prepareValueBytes) {
        throw new UnsupportedOperationException("not supported for this combination of key/value type");
    }

    public OnHeapUpdatableChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment a) {
        throw new UnsupportedOperationException("not supported for this combination of key/value type");
    }

}
