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

import net.openhft.chronicle.ChronicleHash;
import net.openhft.chronicle.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.Byteable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Extension of {@link ConcurrentMap} interface, stores the data off-heap.
 *
 * <p>For information on
 * <ul>
 *     <li>how to construct a {@code ChronicleMap}</li>
 *     <li>{@code ChronicleMap} flavors and properties</li>
 *     <li>available configurations</li>
 * </ul>
 * see {@link ChronicleMapBuilder} documentation.
 *
 * <p>Functionally this interface defines some methods supporting garbage-free off-heap programming:
 * {@link #getUsing(Object, Object)}, {@link #acquireUsing(Object, Object)}.
 *
 * <p>Note that {@code ChronicleMap} extends {@link Closeable}, don't forget
 * to {@linkplain #close() close} map when it is no longer needed.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 */
public interface ChronicleMap<K, V> extends ConcurrentMap<K, V>, ChronicleHash {
    /**
     * Returns the number of entries in this map.
     *
     * @return number of entries in this map
     * @see Map#size()
     */
    long longSize();

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key.
     *
     * <p>If the specified key if absent in the map, {@linkplain
     * ChronicleMapBuilder#defaultValue(Object) default value} is taken or {@linkplain
     * ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider) default value provider}
     * is called (depending on what was configured last in {@code ChronicleMapBuilder}).
     * If it results to something different from {@code null}, this object is put to this map for
     * the specified key, and then returned from this {@code get()} call. Otherwise, if default
     * value is {@code null} and default value provider returns {@code null}, the map remains
     * unchanged and {@code null} is returned.
     *
     * <p>If the value class allows reusing, particularly if it is a {@link Byteable} subclass,
     * consider {@link #getUsing(Object, Object)} method instead of this to reduce garbage creation.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped after this method call,
     * or {@code null} if no value is mapped
     * @see #getUsing(Object, Object)
     */
    @Override
    V get(Object key);

    /**
     * Returns the value to which the specified key is mapped, read to the provided {@code value}
     * object, if possible, or returns {@code null}, if this map contains no mapping for the key.
     *
     * <p>If the specified key is present in the map, the value data is read to the provided
     * {@code value} object via value marshaller's {@link BytesMarshaller#read(Bytes, Object)
     * read(Bytes, value)} or value reader's {@link BytesReader#read(Bytes, long, Object)
     * read(Bytes, size, value)} method, depending on what deserialization strategy is configured
     * on this map builder. If the value deserializer is able to reuse the given {@code value}
     * object, calling this method instead of {@link #get(Object)} could help to reduce garbage
     * creation.
     *
     * <p>The provided {@code value} object is allowed to be {@code null}, in this case
     * {@code map.getUsing(key, null)} call is semantically equivalent to simple
     * {@code map.get(key)} call.
     *
     * <p>If the specified key if absent in the map, {@linkplain
     * ChronicleMapBuilder#defaultValue(Object) default value} is taken or {@linkplain
     * ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider) default value provider}
     * is called. If it results to something different from {@code null}, this object is put
     * to this map for the specified key, and then a new value is returned from this
     * {@code getUsing()} call. Otherwise, if default value is {@code null} and default value
     * provider, if specified, returns {@code null}, the map remains unchanged and {@code null}
     * is returned. The provided {@code value} is untouched anyway in this case.
     *
     * @param key   the key whose associated value is to be returned
     * @param value the object to read value data in, if possible
     * @return the value mapped to the specified key after this method call, or {@code null} if no
     * value is mapped
     * @see #get(Object)
     * @see ChronicleMapBuilder#valueMarshallerAndFactory(BytesMarshaller, ObjectFactory)
     */
    V getUsing(K key, V value);

    /**
     * Acquire a value for a key, creating if absent. If the value is Byteable, it will be assigned
     * to reference the value, instead of copying the data.
     *
     * @param key   the key whose associated value is to be returned
     * @param value to reuse if possible. If null, a new object will be created.
     * @return value created or found.
     */
    V acquireUsing(K key, V value);

}
