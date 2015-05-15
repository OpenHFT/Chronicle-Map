/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Value;

import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.hash.Value.bytesEquivalent;

/**
 * SPI interface for customizing behaviour of the specific Map's methods with individual keys.
 */
public interface MapMethods<K, V> {

    default boolean containsKey(MapQueryContext<K, V> q) {
        return q.entry() != null;
    }

    /**
     * Backing {@link ChronicleMap#get}, {@link ChronicleMap#getUsing} and
     * {@link ChronicleMap#getOrDefault} methods.
     */
    default void get(MapQueryContext<K, V> q, ReturnValue<? super V> returnValue) {
        MapMethodsSupport.returnCurrentValueIfPresent(q, returnValue);
    }

    default boolean put(MapQueryContext<K, V> q,
                        Value<V, ?> value, ReturnValue<? super V> returnValue) {
        // We cannot read the previous value under read lock, because then we will need
        // to release the read lock -> acquire write lock, the value might be updated in
        // between, that will break ConcurrentMap.put() atomicity guarantee. So, we acquire
        // update lock from the start:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            return q.replaceValue(entry, value);
        } else {
            return q.insert(q.absentEntry(), value);
        }
    }

    default boolean putIfAbsent(MapQueryContext<K, V> q,
                                Value<V, ?> value, ReturnValue<? super V> returnValue) {
        if (MapMethodsSupport.tryReturnCurrentValueIfPresent(q, returnValue))
            return false;
        // Key is absent
        return q.insert(q.absentEntry(), value);
    }

    default boolean acquireUsing(MapQueryContext<K, V> q, ReturnValue<? super V> returnValue) {
        if (MapMethodsSupport.tryReturnCurrentValueIfPresent(q, returnValue))
            return false;
        // Key is absent
        Value<V, ?> defaultValue = q.defaultValue(q.absentEntry());
        boolean inserted = q.insert(q.absentEntry(), defaultValue);
        returnValue.returnValue(defaultValue);
        return inserted;
    }

    default boolean remove(MapQueryContext<K, V> q, ReturnValue<? super V> returnValue) {
        // We cannot read the previous value under read lock, because then we will need
        // to release the read lock -> acquire write lock, the value might be updated in
        // between, that will break ConcurrentMap.remove() atomicity guarantee. So, we acquire
        // update lock from the start:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            return q.remove(entry);
        } else {
            return false;
        }
    }

    default boolean remove(MapQueryContext<K, V> q, Value<V, ?> value) {
        // remove(key, value) should find the entry & remove most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        return entry != null &&
                bytesEquivalent(entry.value(), value) &&
                q.remove(entry);
    }

    default boolean replace(MapQueryContext<K, V> q,
                            Value<V, ?> value, ReturnValue<? super V> returnValue) {
        // replace(key, value) should find the key & put the value most of the time,
        // so don't try to check key presence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            return q.replaceValue(entry, value);
        } else {
            return false;
        }
    }

    default boolean replace(MapQueryContext<K, V> q,
                            Value<V, ?> oldValue, Value<V, ?> newValue) {
        // replace(key, old, new) should find the entry & put new value most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        return entry != null &&
                bytesEquivalent(((MapEntry<K, V>) entry).value(), oldValue) &&
                q.replaceValue(entry, newValue);
    }

    default boolean compute(MapQueryContext<K, V> q,
                            BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                            Function<V, Value<V, ?>> newValueObjectToValue,
                            ReturnValue<? super V> returnValue) {
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        V oldValue = entry != null ? entry.value().get() : null;
        V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
        if (newValue != null) {
            Value<V, ?> newValueValue = newValueObjectToValue.apply(newValue);
            boolean updated;
            if (entry != null) {
                updated = q.replaceValue(entry, newValueValue);
            } else {
                updated = q.insert(q.absentEntry(), newValueValue);
            }
            returnValue.returnValue(newValueValue);
            return updated;
        } else if (entry != null) {
            return q.remove(entry);
        } else {
            return false;
        }
    }

    default boolean merge(MapQueryContext<K, V> q, Value<V, ?> value,
                          BiFunction<? super V, ? super V, ? extends V> remappingFunction,
                          Function<V, Value<V, ?>> newValueObjectToValue,
                          ReturnValue<? super V> returnValue) {
        q.updateLock().lock();
        Value<V, ?> newValueValue;
        boolean updated;
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            V oldValue = entry.value().get();
            V newValue = remappingFunction.apply(oldValue, value.get());
            if (newValue == null)
                return q.remove(entry);
            newValueValue = newValueObjectToValue.apply(newValue);
            updated = q.replaceValue(entry, newValueValue);
        } else {
            newValueValue = value;
            updated = q.insert(q.absentEntry(), newValueValue);
        }
        returnValue.returnValue(newValueValue);
        return updated;
    }
}
