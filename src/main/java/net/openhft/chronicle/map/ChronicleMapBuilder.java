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
import net.openhft.chronicle.hash.serialization.AgileBytesMarshaller;
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
 * {@code ChronicleMapBuilder} is intended to be used to configure {@link ChronicleMap}s with
 * non-{@link Byteable} values, which don't point to off-heap memory directly, including primitives
 * ({@link Long}, {@link Double}, etc.), {@link String}s and {@link CharSequence}s, values
 * implementing {@link BytesMarshallable}, {@link Externalizable} or {@link Serializable} interface,
 * or any other values for which {@linkplain #valueMarshaller(BytesMarshaller) custom marshaller}
 * is provided.
 *
 * <p>Use static {@link #of(Class, Class) of(Key.class, Value.class)} method to obtain
 * a {@code ChronicleMapBuilder} instance.
 *
 * @param <K> key type of the maps, created by this builder
 * @param <V> value type of the maps, created by this builder
 * @see OffHeapUpdatableChronicleMapBuilder
 * @see AbstractChronicleMapBuilder
 * @see ChronicleHashBuilder
 */
public final class ChronicleMapBuilder<K, V>
        extends AbstractChronicleMapBuilder<K, V, ChronicleMapBuilder<K, V>> {

    /**
     * Returns a new {@code ChronicleMapBuilder} instance which is able to {@linkplain #create()
     * create} maps with the specified key and value classes.
     *
     * <p>{@code ChronicleMapBuilder} analyzes provided key and value classes and automatically
     * chooses the most specific and effective serializer which it is aware about. Read
     * <a href="https://github.com/OpenHFT/Chronicle-Map#serialization">the section about
     * serialization in Chronicle Map manual</a> for more information.
     *
     * @param keyClass class object used to infer key type and discover it's properties
     *                 via reflection
     * @param valueClass class object used to infer value type and discover it's properties
     *                   via reflection
     * @param <K> key type of the maps, created by the returned builder
     * @param <V> value type of the maps, created by the returned builder
     * @return a new builder for the given key and value classes
     */
    public static <K, V> ChronicleMapBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
        return new ChronicleMapBuilder<K, V>(keyClass, valueClass);
    }

    ChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        super(keyClass, valueClass);
    }

    @Override
    ChronicleMapBuilder<K, V> self() {
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
    public ChronicleMapBuilder<K, V> keySize(int keySize) {
        return super.keySize(keySize);
    }

    /**
     * {@inheritDoc}
     *
     * @see #keySize(int)
     * @see #constantValueSizeBySample(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> constantKeySizeBySample(K sampleKey) {
        return super.constantKeySizeBySample(sampleKey);
    }

    /**
     * Configures the optimal number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. If value size is always the same, call {@link
     * #constantValueSizeBySample(Object)} method instead of this one.
     *
     * <p>If value is a boxed primitive type, i. e. if value size is known statically,
     * it is automatically accounted and shouldn't be specified by user.
     *
     * <p>If value size varies moderately, specify the size higher than average, but lower than the
     * maximum possible, to minimize average memory overuse. If value size varies in a wide range,
     * it's better to use {@linkplain #entrySize(int) entry size} in "chunk" mode and configure it
     * directly.
     *
     * @param valueSize number of bytes, taken by serialized form of values
     * @return this {@code ChronicleMapBuilder} back
     * @see #constantValueSizeBySample(Object)
     * @see #keySize(int)
     * @see #entrySize(int)
     */
    @Override
    public ChronicleMapBuilder<K, V> valueSize(int valueSize) {
        return super.valueSize(valueSize);
    }

    /**
     * Configures the constant number of bytes, taken by serialized form of values, put into maps,
     * created by this builder. This is done by providing the {@code sampleValue}, all values should
     * take the same number of bytes in serialized form, as this sample object.
     *
     * <p>If values are of boxed primitive type or {@link Byteable} subclass, i. e. if value size is
     * known statically, it is automatically accounted and this method shouldn't be called.
     *
     * <p>If value size varies, method {@link #valueSize(int)} or {@link #entrySize(int)} should be
     * called instead of this one.
     *
     * @param sampleValue the sample value
     * @return this builder back
     * @see #valueSize(int)
     * @see #constantKeySizeBySample(Object)
     */

    @Override
    public ChronicleMapBuilder<K, V> constantValueSizeBySample(
            @NotNull V sampleValue) {
        return super.constantValueSizeBySample(sampleValue);
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
     *
     * <p>This serializer is used to serialize both keys and values, if they both require this:
     * loosely typed, nullable, and custom {@linkplain #keyMarshaller(BytesMarshaller) key} and
     * {@linkplain #valueMarshaller(BytesMarshaller) value} marshallers are not configured.
     */
    @Override
    public ChronicleMapBuilder<K, V> objectSerializer(
            @NotNull ObjectSerializer objectSerializer) {
        return super.objectSerializer(objectSerializer);
    }

    /**
     * {@inheritDoc}
     *
     * <p>If {@linkplain #valueMarshaller(BytesMarshaller) custom value marshaller} is configured,
     * this configuration is unused, because it is incapsulated in {@link
     * BytesMarshaller#read(Bytes)} method (without provided instance to read the data into),
     * i. e. it's is the user-side responsibility. Actually this is just a convenience method
     * supporting value marshaller configurations, made initially during {@link #of(Class, Class)}
     * call, if the value class is {@link BytesMarshallable} or {@link Externalizable} subclass.
     *
     * @throws IllegalStateException if custom value marshaller is specified or value class is not
     *         either {@code BytesMarshallable} or {@code Externalizable}
     */
    @Override
    public ChronicleMapBuilder<K, V> valueDeserializationFactory(
            @NotNull ObjectFactory<V> valueDeserializationFactory) {
        return super.valueDeserializationFactory(valueDeserializationFactory);
    }

    /**
     * {@inheritDoc}
     *
     * <p>By default, the default value is set to {@code null}.
     *
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    @Override
    public ChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        return super.defaultValue(defaultValue);
    }

    /**
     * {@inheritDoc}
     *
     * <p>By default, default value provider is not specified, {@link #defaultValue(Object) default
     * value} is specified instead.
     *
     * @see #defaultValue(Object)
     */
    @Override
    public ChronicleMapBuilder<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        return super.defaultValueProvider(defaultValueProvider);
    }

    public ChronicleMapBuilder<K, V> valueMarshaller(
            @NotNull BytesMarshaller<V> valueMarshaller) {
        valueBuilder.marshaller(valueMarshaller, null);
        return this;
    }

    public ChronicleMapBuilder<K, V> valueMarshaller(
            @NotNull AgileBytesMarshaller<V> valueMarshaller) {
        valueBuilder.agileMarshaller(valueMarshaller, null);
        return this;
    }
}
