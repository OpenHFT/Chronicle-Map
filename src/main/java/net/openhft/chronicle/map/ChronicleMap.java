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
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Extension of {@link ConcurrentMap} interface, stores the data off-heap.
 *
 * <p>For information on <ul> <li>how to construct a {@code ChronicleMap}</li> <li>{@code
 * ChronicleMap} flavors and properties</li> <li>available configurations</li> </ul> see {@link
 * AbstractChronicleMapBuilder} documentation.
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
     * @see OnHeapUpdatableChronicleMapBuilder#valueMarshaller(BytesMarshaller)
     */
    V getUsing(K key, V usingValue);

    /**
     * The method is similar {@link  V getUsing(K key, V usingValue);}  but in addition read locks
     * the map segment to operations to be performed atomically. ( see the example below )
     *
     * Returns the  ReadContext which holds a map segment lock and provides method to get the value
     * ( to which the specified key is mapped)  atomically ( see example below for an explanation ),
     * read to the provided {@code value} object, if possible, or returns {@code null}, if this map
     * contains no mapping for the key.
     *
     * <p>If the specified key is present in the map, the readContext.value() uses the provided
     * {@code usingValue} object via value marshaller's {@link BytesMarshaller#read(Bytes, Object)
     * read(Bytes, value)} or value reader's {@link BytesReader#read(Bytes, long, Object)
     * read(Bytes, size, value)} method, depending on what deserialization strategy is configured on
     * the builder, using which this map was constructed. If the value deserializer is able to reuse
     * the given {@code usingValue} object, calling this method instead of {@link #get(Object)}
     * could help to reduce garbage creation.
     * <pre>{@code
     * try (ReadContext rc = map.getUsingLocked(key, bond)) {
     *    if (rc.present ()) { // check whether the key was present
     *    long issueDate = bond.getIssueDate();
     *    String symbol = bond.getSymbol();
     * // add your logic here ( the lock will ensure this bond can not be changed by another thread
     * )
     * }
     * } // the read lock is released here
     * }</pre>
     * To ensure that you can read the 'issueDate' and 'symbol' can be read atomically, these values
     * must be read while the segment lock is in place.
     *
     *
     * <p>The provided {@code value} object is allowed to be {@code null}, in this case
     *
     * @param key        the key whose associated value is to be returned
     * @param usingValue the object to read value data in, if possible
     * @return the read context containing the value to which the specified key is mapped
     *
     * no mapping for the key
     * @see #get(Object)
     * @see #getUsing(Object, Object)
     * @see OnHeapUpdatableChronicleMapBuilder#valueMarshaller(BytesMarshaller)
     */
    @NotNull
    ReadContext<K, V> getUsingLocked(@NotNull K key, @NotNull V usingValue);

    /**
     * Acquire a value for a key, creating if absent.
     *
     * <p>If the specified key is absent in the map, {@linkplain AbstractChronicleMapBuilder#defaultValue(Object)
     * default value} is taken or {@linkplain AbstractChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)
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
     * Where {@code defaultValue(key)} returns either {@linkplain AbstractChronicleMapBuilder#defaultValue(Object)
     * default value} or {@link AbstractChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)
     * defaultValueProvider.}
     *
     * <p>If the {@code ChronicleMap} is off-heap updatable, i. e. created via {@link
     * ChronicleMapBuilder} builder (values are {@link Byteable}), there is one more option of what
     * to do if the key is absent in the map, see {@link ChronicleMapBuilder#prepareValueBytesOnAcquire(PrepareValueBytes)}.
     * By default, value bytes are just zeroed out, no default value, either provided for key or
     * constant, is put for the absent key.
     *
     * @param key        the key whose associated value is to be returned
     * @param usingValue the object to read value data in, if present. Can not be null
     * @return value to which the given key is mapping after this call, either found or created
     * @see #getUsing(Object, Object)
     */
    V acquireUsing(@NotNull K key, V usingValue);

    @NotNull
    WriteContext<K, V> acquireUsingLocked(@NotNull K key, @NotNull V usingValue);

    /**
     * Apply a mapping to the value returned by a key and return a result. A read lock is assumed.
     *
     * @param key      to apply the mapping to
     * @param function to calculate a result
     * @param <R>      return type.
     * @return the result of the function, or null if there is no entry for the key.
     */
    <R> R mapForKey(K key, @NotNull Function<? super V, R> function);

    /**
     * Apply a mutator to the value for a key and return a result. A write lock is assumed. <p> If
     * there is no entry for this key it will be created and the value empty.
     *
     * @param key     to apply the mapping to
     * @param mutator to alter the value and calculate a result
     * @param <R>     return type.
     * @return the result of the function.
     */
    <R> R updateForKey(K key, @NotNull Mutator<? super V, R> mutator);

    /**
     * Returns a {@link java.util.concurrent.Future} containing the value to which the specified key
     * is mapped, or {@code null} if this map contains no mapping for the key. This method behaves
     * the same as {@link java.util.Map#get(java.lang.Object)} yet wraps result in a future a {@link
     * java.util.concurrent.Future}.
     *
     * Calling this method may or may not block until the operating is complete depending on the
     * implementation.
     *
     * @param key the key whose associated value is to be returned in the future
     * @return a future containing the value to which the specified key is mapped after this method
     * call, or {@code null} if no value is mapped
     * @see java.util.Map#get(java.lang.Object)
     * @see java.util.concurrent.Future
     */
    Future<V> getLater(@NotNull K key);

    /**
     * Associates the specified value with the specified key in this map (optional operation).  If
     * the map previously contained a mapping for the key, the old value is replaced by the
     * specified value.  (A map <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and
     * only if {@link #containsKey(Object) m.containsKey(k)} would return <tt>true</tt>.)
     *
     * Calling this method may or may not block until the operating is complete depending on the
     * implementation.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key * @return a future containing the
     *              previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no
     *              mapping for <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map
     *              previously associated <tt>null</tt> with <tt>key</tt>, if the implementation
     *              supports <tt>null</tt> values.)
     * @return a future containing either, the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     * @see java.util.concurrent.Future
     */
    Future<V> putLater(@NotNull K key, @NotNull V value);

    /**
     * Removes the mapping for a key from this map if it is present (optional operation).   More
     * formally, if this map contains a mapping from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.  (The map can
     * contain at most one such mapping.)
     *
     * <p>Returns a future containing, either the value to which this map previously associated the
     * key, or <tt>null</tt> if the map contained no mapping for the key.
     *
     * This method behaves the same as {@link java.util.Map#remove(java.lang.Object)} yet wraps
     * result in a future a {@link java.util.concurrent.Future}.
     *
     * Calling this method may or may not block until the operating is complete depending on the
     * implementation.
     *
     * @param key key whose mapping is to be removed from the map
     * @return a future containing either, the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see java.util.Map#remove(java.lang.Object)
     * @see java.util.concurrent.Future
     */
    Future<V> removeLater(@NotNull K key);

    /**
     * exports all the entries to a {@link java.io.File} storing them in JSON format, an attempt is
     * made where possible to use standard java serialisation and keep the data human readable, data
     * serialized using the custom serialises are converted to a binary format which is not human
     * readable but this is only done if the Keys or Values are not {@link java.io.Serializable}.
     * This method can be used in conjunction with {@link ChronicleMap#putAll(java.io.File)} and is
     * especially useful if you wish to import/export entries from one chronicle map into another.
     * This import and export of the entries can be performed even when the versions of ChronicleMap
     * differ. This method is not performant and as such we recommend it is not used in performance
     * sensitive code.
     *
     * MAPS CONTAINING KEYS/VALUES GENERATED WITH OFF-HEAP INTERFACES ARE NOT CURRENTLY SUPPORTED,
     * CURRENTLY ONLY KEYS/VALUES THAT IMPLEMENT SERIALIZABLE ARE SUPPORTED
     *
     * @param toFile the file to store all the entries to, the entries will be stored in JSON
     *               format
     * @throws IOException its not possible store the data to {@code toFile}
     * @see ChronicleMap#putAll(java.io.File)
     */
    void getAll(File toFile) throws IOException;

    /**
     * imports all the entries from a {@link java.io.File}, the {@code fromFile} must be created
     * using or the same format as {@link ChronicleMap#get(java.lang.Object)}, this method behaves
     * similar to {@link java.util.Map#put(java.lang.Object, java.lang.Object)} where existing
     * entries are overwritten. A write lock is only held while each individual entry is inserted
     * into the map, not over all the entries in the {@link java.io.File}
     *
     * MAPS CONTAINING KEYS/VALUES GENERATED WITH OFF-HEAP INTERFACES ARE NOT CURRENTLY SUPPORTED,
     * CURRENTLY ONLY KEYS/VALUES THAT IMPLEMENT SERIALIZABLE ARE SUPPORTED
     *
     * @param fromFile the file containing entries ( in JSON format ) which will be deserialized and
     *                 {@link java.util.Map#put(java.lang.Object, java.lang.Object)} into the map
     * @throws IOException its not possible read the {@code fromFile}
     * @see ChronicleMap#getAll(java.io.File)
     */
    void putAll(File fromFile) throws IOException;

}

