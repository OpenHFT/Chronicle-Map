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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ExternalHashQueryContext;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Object;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Extension of {@link ConcurrentMap} interface, stores the data off-heap.
 *
 * <p>For information on <ul> <li>how to construct a {@code ChronicleMap}</li> <li>{@code
 * ChronicleMap} flavors and properties</li> <li>available configurations</li> </ul> see {@link
 * ChronicleMapBuilder} documentation.
 *
 * <p>Functionally this interface defines some methods supporting garbage-free off-heap programming:
 * {@link #getUsing(Object, Object)}, {@link #acquireUsing(Object, Object)}.
 *
 * <p>Roughly speaking, {@code ChronicleMap} compares keys and values by their binary serialized
 * form, that shouldn't necessary be the same equality relation as defined by built-in {@link
 * Object#equals(Object)} method, which is prescribed by general {@link Map} contract.
 *
 * <p>Note that {@code ChronicleMap} extends {@link Closeable}, don't forget to {@linkplain #close()
 * close} map when it is no longer needed.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 */
public interface ChronicleMap<K, V>
        extends ConcurrentMap<K, V>,
        ChronicleHash<K, MapKeyContext<K, V>, ExternalMapQueryContext<K, V, ?>> {

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key.
     *
     * <p>If the value class allows reusing, particularly if it is a {@link Byteable} subclass,
     * consider {@link #getUsing(Object, Object)} method instead of this to reduce garbage
     * creation.
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
     *
     * <p>If the specified key is present in the map, the value data is read to the provided {@code
     * value} object via value marshaller's {@link BytesMarshaller#read(Bytes, Object) read(Bytes,
     * value)} or value reader's {@link BytesReader#read(Bytes, long, Object) read(Bytes, size,
     * value)} method, depending on what deserialization strategy is configured on the builder,
     * using which this map was constructed. If the value deserializer is able to reuse the given
     * {@code value} object, calling this method instead of {@link #get(Object)} could help to
     * reduce garbage creation.
     *
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
     * @see ChronicleMapBuilder#valueMarshaller(BytesMarshaller)
     */
    V getUsing(K key, V usingValue);

    /**
     * Acquire a value for a key, creating if absent.
     *
     * <p>If the specified key is absent in the map, {@linkplain ChronicleMapBuilder#defaultValue(Object)
     * default value} is taken or {@linkplain ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)
     * default value provider} is called. Then this object is put to this map for the specified
     * key.
     *
     * <p>Then, either if the key was initially absent in the map or already present, the value is
     * deserialized just as during {@link #getUsing(Object, Object) getUsing(key, usingValue)} call,
     * passed the same {@code key} and {@code usingValue} as into this method call. This means, as
     * in {@link #getUsing}, {@code usingValue} could safely be {@code null}, in this case a new
     * value instance is created to deserialize the data.
     *
     * In code, {@code acquireUsing} is specified as :
     * <pre>{@code
     * V acquireUsing(K key, V usingValue) {
     *     if (!containsKey(key))
     *         put(key, defaultValue(key));
     *     return getUsing(key, usingValue);
     * }}</pre>
     *
     *
     * Where {@code defaultValue(key)} returns either {@linkplain ChronicleMapBuilder#defaultValue(Object)
     * default value} or {@link ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)
     * defaultValueProvider.}
     *
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

    /**
     * @deprecated this method has incoherent, unclear semantics. Use {@link #queryContext(Object)}
     * instead
     */
    @Deprecated
    @NotNull
    MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue);

    /**
     * Apply a mapping to the value returned by a key and return a result. A read lock is assumed.
     *
     * @param key      to apply the mapping to
     * @param function to calculate a result
     * @param <R>      return type.
     * @return the result of the function, or null if there is no entry for the key.
     */
    <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function);

    /**
     * Apply a unaryOperator to the value for a key and return a result. A write lock is assumed.
     * <p> If there is no entry for this key null will be returned
     *
     * @param key           to apply the mapping to
     * @param unaryOperator to alter the value and calculate a result
     * @return the result of the function.
     */
    V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator);

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
     * Creates an empty value instance, which can be used with the
     * following methods :
     *
     * {@link ChronicleMap#getUsing(Object, Object) }
     * {@link ChronicleMap#acquireUsing(Object, Object)      }
     * {@link ChronicleMap#acquireContext(Object, Object)  }
     *
     * for example like this :
     *
     *  <pre>{@code
     * V value = map.newValueInstance();
     * try (ReadMapContext rc = map.getUsingLocked(key, value)) {
     *  // add your logic here
     * } // the read lock is released here
     * }</pre>
     *
     *
     * @return a new empty instance based on the Value type
     * @see ChronicleMap#getUsing(Object, Object)
     * @see ChronicleMap#acquireUsing(Object, Object)
     * @see ChronicleMap#acquireContext(Object, Object)
     */
    V newValueInstance();

    /**
     * Creates an empty value instance, which can be used with the
     * following methods :
     *
     * {@link ChronicleMap#getUsing(Object, Object) }
     * {@link ChronicleMap#acquireUsing(Object, Object)      }
     * {@link ChronicleMap#acquireContext(Object, Object)  }
     *
     * for example like this :
     *
     *  <pre>{@code
     * K key = map.newKeyInstance();
     * key.setMyStringField("some key");
     *
     * try (ReadMapContext rc = map.getUsingLocked(key, value)) {
     *  // add your logic here
     * } // the read lock is released here
     * }</pre>
     *
     *
     * @return a new empty instance based on the Key type
     * @see ChronicleMap#getUsing(Object, Object)
     * @see ChronicleMap#acquireUsing(Object, Object)
     * @see ChronicleMap#acquireContext(Object, Object)
     */
    K newKeyInstance();

    /**
     * @return the class of {@code <V>}
     */
    Class<V> valueClass();
}

