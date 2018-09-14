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

import net.openhft.chronicle.hash.Data;

import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.hash.Data.bytesEquivalent;
import static net.openhft.chronicle.map.MapMethodsSupport.returnCurrentValueIfPresent;
import static net.openhft.chronicle.map.MapMethodsSupport.tryReturnCurrentValueIfPresent;

/**
 * SPI interface for customizing behaviour of the specific Map's methods with individual keys.
 *
 * @see ChronicleMapBuilder#mapMethods(MapMethods)
 */
public interface MapMethods<K, V, R> {

    /**
     * Backing {@link ChronicleMap#containsKey(Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * return q.entry() != null;
     * }</pre>
     */
    default boolean containsKey(MapQueryContext<K, V, R> q) {
        return q.entry() != null;
    }

    /**
     * Backing {@link ChronicleMap#get}, {@link ChronicleMap#getUsing} and
     * {@link ChronicleMap#getOrDefault} methods.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null)
     *     returnValue.returnValue(entry.value());
     * }</pre>
     */
    default void get(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        returnCurrentValueIfPresent(q, returnValue);
    }

    /**
     * Backing {@link ChronicleMap#put(Object, Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * // We cannot read the previous value under read lock, because then we will need
     * // to release the read lock -> acquire write lock, the value might be updated in
     * // between, that will break ConcurrentMap.put() atomicity guarantee. So, we acquire
     * // update lock from the start:
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     q.replaceValue(entry, value);
     * } else {
     *     q.insert(q.absentEntry(), value);
     * }}</pre>
     */
    default void put(MapQueryContext<K, V, R> q, Data<V> value, ReturnValue<V> returnValue) {
        // We cannot read the previous value under read lock, because then we will need
        // to release the read lock -> acquire write lock, the value might be updated in
        // between, that will break ConcurrentMap.put() atomicity guarantee. So, we acquire
        // update lock from the start:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            q.replaceValue(entry, value);
        } else {
            q.insert(q.absentEntry(), value);
        }
    }

    /**
     * Backing {@link ChronicleMap#putIfAbsent(Object, Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * if (q.readLock().tryLock()) {
     *     MapEntry<K, V> entry = q.entry();
     *     if (entry != null) {
     *         returnValue.returnValue(entry.value());
     *         return;
     *     }
     *     // Key is absent
     *     q.readLock().unlock();
     * }
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     return;
     * }
     * // Key is absent
     * q.insert(q.absentEntry(), value);
     * }</pre>
     */
    default void putIfAbsent(MapQueryContext<K, V, R> q,
                             Data<V> value, ReturnValue<V> returnValue) {
        if (tryReturnCurrentValueIfPresent(q, returnValue))
            return;
        // Key is absent
        q.insert(q.absentEntry(), value);
    }

    /**
     * Backing {@link ChronicleMap#acquireUsing(Object, Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * if (q.readLock().tryLock()) {
     *     MapEntry<K, V> entry = q.entry();
     *     if (entry != null) {
     *         returnValue.returnValue(entry.value());
     *         return;
     *     }
     *     // Key is absent
     *     q.readLock().unlock();
     * }
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     return;
     * }
     * // Key is absent
     * q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
     * // Meaningful to return the default as newly-inserted, not the default entry itself.
     * // map.acquireUsing() is most useful for value interfaces, for which it makes big
     * // difference -- what bytes to refer. Consider map.acquireUsing(...).incrementValue();
     * returnValue.returnValue(q.entry().value());
     * }</pre>
     */
    default void acquireUsing(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        if (tryReturnCurrentValueIfPresent(q, returnValue))
            return;
        // Key is absent
        q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
        // meaningful to return the default as newly-inserted, not the default entry itself.
        // map.acquireUsing() is most useful for value interfaces, for which it makes big
        // difference -- what bytes to refer. consider map.acquireUsing(...).incrementValue();
        // The same reasoning is applied in all same occurrences in this class file
        returnValue.returnValue(q.entry().value());
    }

    /**
     * Backing {@link ChronicleMap#computeIfAbsent(Object, Function)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * if (q.readLock().tryLock()) {
     *     MapEntry<K, V> entry = q.entry();
     *     if (entry != null) {
     *         returnValue.returnValue(entry.value());
     *         return;
     *     }
     *     // Key is absent
     *     q.readLock().unlock();
     * }
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     return;
     * }
     * // Key is absent
     * q.insert(q.absentEntry(), q.wrapValueAsData(mappingFunction.apply(q.queriedKey().get())));
     * returnValue.returnValue(q.entry().value());
     * }</pre>
     */
    default void computeIfAbsent(MapQueryContext<K, V, R> q,
                                 Function<? super K, ? extends V> mappingFunction,
                                 ReturnValue<V> returnValue) {
        if (tryReturnCurrentValueIfPresent(q, returnValue))
            return;
        // Key is absent
        q.insert(q.absentEntry(), q.wrapValueAsData(mappingFunction.apply(q.queriedKey().get())));
        returnValue.returnValue(q.entry().value());
    }

    /**
     * Backing {@link ChronicleMap#remove(Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * // We cannot read the previous value under read lock, because then we will need
     * // to release the read lock -> acquire write lock, the value might be updated in
     * // between, that will break ConcurrentMap.remove() atomicity guarantee. So, we acquire
     * // update lock from the start:
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     q.remove(entry);
     * }}</pre>
     */
    default void remove(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        // We cannot read the previous value under read lock, because then we will need
        // to release the read lock -> acquire write lock, the value might be updated in
        // between, that will break ConcurrentMap.remove() atomicity guarantee. So, we acquire
        // update lock from the start:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            q.remove(entry);
        }
    }

    /**
     * Backing {@link ChronicleMap#remove(Object, Object)} method.
     *
     * @return if the entry was removed
     * @implNote the default implementation is equivalent to <pre>{@code
     * // remove(key, value) should find the entry & remove most of the time,
     * // so don't try to check key presence and value equivalence under read lock first,
     * // as in putIfAbsent()/acquireUsing(), start with update lock:
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null && Data.bytesEquivalent(entry.value(), value)) {
     *     q.remove(entry);
     *     return true;
     * } else {
     *     return false;
     * }}</pre>
     */
    default boolean remove(MapQueryContext<K, V, R> q, Data<V> value) {
        // remove(key, value) should find the entry & remove most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null && bytesEquivalent(value, entry.value())) {
            q.remove(entry);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Backing {@link ChronicleMap#replace(Object, Object)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * // replace(key, value) should find the key & put the value most of the time,
     * // so don't try to check key presence under read lock first,
     * // as in putIfAbsent()/acquireUsing(), start with update lock:
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     returnValue.returnValue(entry.value());
     *     q.replaceValue(entry, value);
     * }}</pre>
     */
    default void replace(MapQueryContext<K, V, R> q,
                         Data<V> value, ReturnValue<V> returnValue) {
        // replace(key, value) should find the key & put the value most of the time,
        // so don't try to check key presence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            q.replaceValue(entry, value);
        }
    }

    /**
     * Backing {@link ChronicleMap#replace(Object, Object, Object)} method.
     *
     * @return if the entry was replaced
     * @implNote the default implementation is equivalent to <pre>{@code
     * // replace(key, old, new) should find the entry & put new value most of the time,
     * // so don't try to check key presence and value equivalence under read lock first,
     * // as in putIfAbsent()/acquireUsing(), start with update lock:
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null && Data.bytesEquivalent(((MapEntry<K, V>) entry).value(), oldValue)) {
     *     q.replaceValue(entry, newValue);
     *     return true;
     * } else {
     *     return false;
     * }}</pre>
     */
    default boolean replace(MapQueryContext<K, V, R> q,
                            Data<V> oldValue, Data<V> newValue) {
        // replace(key, old, new) should find the entry & put new value most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null && bytesEquivalent(oldValue, entry.value())) {
            q.replaceValue(entry, newValue);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Backing {@link ChronicleMap#compute(Object, BiFunction)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * V oldValue = entry != null ? entry.value().get() : null;
     * V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
     * if (newValue != null) {
     *     Data<V, ?> newValueData = q.wrapValueAsData(newValue);
     *     if (entry != null) {
     *         q.replaceValue(entry, newValueData);
     *     } else {
     *         q.insert(q.absentEntry(), newValueData);
     *         entry = q.entry();
     *     }
     *     returnValue.returnValue(entry.value());
     * } else if (entry != null) {
     *     q.remove(entry);
     * }}</pre>
     */
    default void compute(MapQueryContext<K, V, R> q,
                         BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                         ReturnValue<V> returnValue) {
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        V oldValue = entry != null ? entry.value().get() : null;
        V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
        if (newValue != null) {
            Data<V> newValueData = q.wrapValueAsData(newValue);
            if (entry != null) {
                q.replaceValue(entry, newValueData);
            } else {
                q.insert(q.absentEntry(), newValueData);
                entry = q.entry();
                assert entry != null;
            }
            returnValue.returnValue(entry.value());
        } else if (entry != null) {
            q.remove(entry);
        }
    }

    /**
     * Backing {@link ChronicleMap#computeIfPresent(Object, BiFunction)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * q.updateLock().lock();
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     V oldValue = entry.value().get();
     *     V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
     *     if (newValue != null ) {
     *         q.replaceValue(entry, q.wrapValueAsData(newValue));
     *         returnValue.returnValue(q.entry().value());
     *     } else {
     *         q.remove(entry);
     *     }
     * }}</pre>
     */
    default void computeIfPresent(MapQueryContext<K, V, R> q,
                                  BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                                  ReturnValue<V> returnValue) {
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            V oldValue = entry.value().get();
            V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
            if (newValue != null) {
                q.replaceValue(entry, q.wrapValueAsData(newValue));
                returnValue.returnValue(q.entry().value());
            } else {
                q.remove(entry);
            }
        }
    }

    /**
     * Backing {@link ChronicleMap#merge(Object, Object, BiFunction)} method.
     *
     * @implNote the default implementation is equivalent to <pre>{@code
     * q.updateLock().lock();
     * Data<V, ?> newValueData;
     * MapEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     V oldValue = entry.value().get();
     *     V newValue = remappingFunction.apply(oldValue, value.get());
     *     if (newValue == null) {
     *         q.remove(entry);
     *         return;
     *     }
     *     newValueData = q.wrapValueAsData(newValue);
     *     q.replaceValue(entry, newValueData);
     * } else {
     *     newValueData = value;
     *     q.insert(q.absentEntry(), newValueData);
     *     entry = q.entry();
     * }
     * returnValue.returnValue(entry.value());
     * }</pre>
     */
    default void merge(MapQueryContext<K, V, R> q, Data<V> value,
                       BiFunction<? super V, ? super V, ? extends V> remappingFunction,
                       ReturnValue<V> returnValue) {
        q.updateLock().lock();
        Data<V> newValueData;
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            V oldValue = entry.value().get();
            V newValue = remappingFunction.apply(oldValue, value.get());
            if (newValue == null) {
                q.remove(entry);
                return;
            }
            newValueData = q.wrapValueAsData(newValue);
            q.replaceValue(entry, newValueData);
        } else {
            newValueData = value;
            q.insert(q.absentEntry(), newValueData);
            entry = q.entry();
            assert entry != null;
        }
        returnValue.returnValue(entry.value());
    }
}
