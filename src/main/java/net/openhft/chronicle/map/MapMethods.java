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

import static net.openhft.chronicle.hash.Value.bytesEquivalent;
import static net.openhft.chronicle.map.MapMethodsSupport.returnCurrentValueIfPresent;
import static net.openhft.chronicle.map.MapMethodsSupport.tryReturnCurrentValueIfPresent;

/**
 * SPI interface for customizing behaviour of the specific Map's methods with individual keys.
 */
public interface MapMethods<K, V, R> {

    default boolean containsKey(MapQueryContext<K, V, R> q) {
        return q.entry() != null;
    }

    /**
     * Backing {@link ChronicleMap#get}, {@link ChronicleMap#getUsing} and
     * {@link ChronicleMap#getOrDefault} methods.
     */
    default void get(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        returnCurrentValueIfPresent(q, returnValue);
    }

    default void put(MapQueryContext<K, V, R> q, Value<V, ?> value, ReturnValue<V> returnValue) {
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

    default void putIfAbsent(MapQueryContext<K, V, R> q,
                             Value<V, ?> value, ReturnValue<V> returnValue) {
        if (tryReturnCurrentValueIfPresent(q, returnValue))
            return;
        // Key is absent
        q.insert(q.absentEntry(), value);
    }

    default void acquireUsing(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        if (tryReturnCurrentValueIfPresent(q, returnValue))
            return;
        // Key is absent
        q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
        // meaningful to return the default as newly-inserted, not the default entry itself.
        // map.acquireUsing() is most useful for data-value generated values, for which it makes
        // big difference -- what bytes to refer. map.acquireUsing().incrementValue();
        returnValue.returnValue(q.entry().value());
    }

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

    default boolean remove(MapQueryContext<K, V, R> q, Value<V, ?> value) {
        // remove(key, value) should find the entry & remove most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null && bytesEquivalent(entry.value(), value)) {
            q.remove(entry);
            return true;
        } else {
            return false;
        }
    }

    default void replace(MapQueryContext<K, V, R> q,
                         Value<V, ?> value, ReturnValue<V> returnValue) {
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

    default boolean replace(MapQueryContext<K, V, R> q,
                            Value<V, ?> oldValue, Value<V, ?> newValue) {
        // replace(key, old, new) should find the entry & put new value most of the time,
        // so don't try to check key presence and value equivalence under read lock first,
        // as in putIfAbsent()/acquireUsing(), start with update lock:
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null && bytesEquivalent(((MapEntry<K, V>) entry).value(), oldValue)) {
            q.replaceValue(entry, newValue);
            return true;
        } else {
            return false;
        }
    }

    default void compute(MapQueryContext<K, V, R> q,
                            BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                            ReturnValue<V> returnValue) {
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        V oldValue = entry != null ? entry.value().get() : null;
        V newValue = remappingFunction.apply(q.queriedKey().get(), oldValue);
        if (newValue != null) {
            Value<V, ?> newValueValue = q.wrapValueAsValue(newValue);
            if (entry != null) {
                q.replaceValue(entry, newValueValue);
            } else {
                q.insert(q.absentEntry(), newValueValue);
            }
            returnValue.returnValue(newValueValue);
        } else if (entry != null) {
            q.remove(entry);
        }
    }

    default void merge(MapQueryContext<K, V, R> q, Value<V, ?> value,
                          BiFunction<? super V, ? super V, ? extends V> remappingFunction,
                          ReturnValue<V> returnValue) {
        q.updateLock().lock();
        Value<V, ?> newValueValue;
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            V oldValue = entry.value().get();
            V newValue = remappingFunction.apply(oldValue, value.get());
            if (newValue == null) {
                q.remove(entry);
                return;
            }
            newValueValue = q.wrapValueAsValue(newValue);
            q.replaceValue(entry, newValueValue);
        } else {
            newValueValue = value;
            q.insert(q.absentEntry(), newValueValue);
        }
        returnValue.returnValue(newValueValue);
    }
}
