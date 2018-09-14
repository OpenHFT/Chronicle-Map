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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * {@code ChronicleMap} provides concurrent access to a <i>Chronicle Map key-value store</i> from a
 * JVM process.
 * <p>
 * <p>For information on <ul> <li>how to construct a {@code ChronicleMap}</li> <li>{@code
 * ChronicleMap} flavors and properties</li> <li>available configurations</li> </ul> see {@link
 * ChronicleMapBuilder} documentation.
 * <p>
 * <p>Functionally this interface defines some methods supporting garbage-free off-heap programming:
 * {@link #getUsing(Object, Object)}, {@link #acquireUsing(Object, Object)}.
 * <p>
 * <p>Roughly speaking, {@code ChronicleMap} compares keys and values by their binary serialized
 * form, that shouldn't necessary be the same equality relation as defined by built-in {@link
 * Object#equals(Object)} method, which is prescribed by general {@link Map} contract.
 * <p>
 * <p>Note that {@code ChronicleMap} extends {@link Closeable}, don't forget to {@linkplain #close()
 * close} map when it is no longer needed.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see ChronicleMapBuilder#create()
 * @see ChronicleMapBuilder#createPersistedTo(File)
 * @see ChronicleMapBuilder#createOrRecoverPersistedTo(File, boolean)
 */
public interface ChronicleMap<K, V> extends ConcurrentMap<K, V>,
        ChronicleHash<K, MapEntry<K, V>, MapSegmentContext<K, V, ?>,
                ExternalMapQueryContext<K, V, ?>> {

    /**
     * Delegates to {@link ChronicleMapBuilder#of(Class, Class)} for convenience.
     *
     * @param keyClass   class of the key type of the Chronicle Map to create
     * @param valueClass class of the value type of the Chronicle Map to create
     * @param <K>        the key type of the Chronicle Map to create
     * @param <V>        the value type of the Chronicle Map to create
     * @return a new {@code ChronicleMapBuilder} for the given key and value classes
     */
    static <K, V> ChronicleMapBuilder<K, V> of(Class<K> keyClass, Class<V> valueClass) {
        return ChronicleMapBuilder.of(keyClass, valueClass);
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key.
     * <p>
     * <p>If the value class allows reusing, consider {@link #getUsing(Object, Object)} method
     * instead of this to reduce garbage creation. Read <a
     * href="https://github.com/OpenHFT/Chronicle-Map#single-key-queries">the section about usage
     * patterns in the Chronicle Map 3 Tutorial</a> for more.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped after this method call, or {@code
     * null} if no value is mapped
     * @see #getUsing(Object, Object)
     */
    @Override
    V get(Object key);

    /**
     * Returns the value to which the specified key is mapped, read to the provided {@code value}
     * object, if possible, or returns {@code null}, if this map contains no mapping for the key.
     * <p>
     * <p>If the specified key is present in the map, the value data is read to the provided {@code
     * value} object via value reader's {@link SizedReader#read(net.openhft.chronicle.bytes.Bytes, long, Object)
     * read(StreamingDataInput, size, value)} method. If the value deserializer is able to reuse the
     * given {@code value} object, calling this method instead of {@link #get(Object)} could help to
     * reduce garbage creation.
     * <p>
     * <p>The provided {@code value} object is allowed to be {@code null}, in this case {@code
     * map.getUsing(key, null)} call is semantically equivalent to simple {@code map.get(key)}
     * call.
     *
     * @param key        the key whose associated value is to be returned
     * @param usingValue the object to read value data in, if possible
     * @return the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key
     * @see #get(Object)
     * @see #acquireUsing(Object, Object)
     * @see ChronicleMapBuilder#valueMarshallers(SizedReader, SizedWriter)
     */
    V getUsing(K key, V usingValue);

    /**
     * Acquire a value for a key, creating if absent.
     * <p>
     * <p>If the specified key is absent in the map, {@linkplain
     * ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider) default value provider} is
     * called. Then this object is put to this map for the specified key.
     * <p>
     * <p>Then, either if the key was initially absent in the map or already present, the value is
     * deserialized just as during {@link #getUsing(Object, Object) getUsing(key, usingValue)} call,
     * passed the same {@code key} and {@code usingValue} as into this method call. This means, as
     * in {@link #getUsing}, {@code usingValue} could safely be {@code null}, in this case a new
     * value instance is created to deserialize the data.
     * <p>
     * <p>In code, {@code acquireUsing} is specified as :
     * <pre>{@code
     * V acquireUsing(K key, V usingValue) {
     *     if (!containsKey(key))
     *         put(key, defaultValue(key));
     *     return getUsing(key, usingValue);
     * }}</pre>
     * <p>
     * <p>
     * <p>Where {@code defaultValue(key)} returns {@link
     * ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider) defaultValueProvider}.
     * <p>
     * <p>If the {@code ChronicleMap} is off-heap updatable, i. e. created via {@link
     * ChronicleMapBuilder} builder (values are {@link Byteable}), there is one more option of what
     * to do if the key is absent in the map. By default, value bytes are just zeroed out, no
     * default value, either provided for key or constant, is put for the absent key.
     *
     * @param key        the key whose associated value is to be returned
     * @param usingValue the object to read value data in, if present. Can not be null
     * @return value to which the given key is mapping after this call, either found or created
     * @see #getUsing(Object, Object)
     */
    V acquireUsing(@NotNull K key, V usingValue);

    @NotNull
    net.openhft.chronicle.core.io.Closeable acquireContext(@NotNull K key, @NotNull V usingValue);

    /**
     * Returns the result of application of the given function to the value to which the given key
     * is mapped. If there is no mapping for the key, {@code null} is returned from {@code
     * getMapped()} call without application of the given function. This method is primarily useful
     * when accessing {@code ChronicleMap} implementation which delegates it's requests to some
     * remote node (server) and pulls the result through serialization/deserialization path, and
     * probably network. In this case, when you actually need only a part of the map value's state
     * (e. g. a single field) it's cheaper to extract it on the server side and transmit lesser
     * bytes.
     *
     * @param key      the key whose associated value is to be queried
     * @param function a function to transform the value to the actually needed result,
     *                 which should be smaller than the map value
     * @param <R>      the result type
     * @return the result of applying the function to the mapped value, or {@code null} if there
     * is no mapping for the key
     */
    <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function);

    /**
     * Exports all the entries to a {@link File} storing them in JSON format, an attempt is
     * made where possible to use standard java serialisation and keep the data human readable, data
     * serialized using the custom serialises are converted to a binary format which is not human
     * readable but this is only done if the Keys or Values are not {@link Serializable}.
     * This method can be used in conjunction with {@link ChronicleMap#putAll(File)} and is
     * especially useful if you wish to import/export entries from one chronicle map into another.
     * This import and export of the entries can be performed even when the versions of ChronicleMap
     * differ. This method is not performant and as such we recommend it is not used in performance
     * sensitive code.
     *
     * @param toFile the file to store all the entries to, the entries will be stored in JSON
     *               format
     * @throws IOException its not possible store the data to {@code toFile}
     * @see ChronicleMap#putAll(File)
     */
    void getAll(File toFile) throws IOException;

    /**
     * Imports all the entries from a {@link File}, the {@code fromFile} must be created
     * using or the same format as {@link ChronicleMap#get(Object)}, this method behaves
     * similar to {@link Map#put(Object, Object)} where existing
     * entries are overwritten. A write lock is only held while each individual entry is inserted
     * into the map, not over all the entries in the {@link File}
     *
     * @param fromFile the file containing entries ( in JSON format ) which will be deserialized and
     *                 {@link Map#put(Object, Object)} into the map
     * @throws IOException its not possible read the {@code fromFile}
     * @see ChronicleMap#getAll(File)
     */
    void putAll(File fromFile) throws IOException;

    /**
     * @return the class of {@code <V>}
     */
    Class<V> valueClass();
}

