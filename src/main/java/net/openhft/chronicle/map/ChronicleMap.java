/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.collections.ConcurrentMapLamdadSupport;
import net.openhft.collections.SharedHashMap;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public interface ChronicleMap<K, V> extends SharedHashMap<K, V>, ConcurrentMapLamdadSupport<K, V> {



    /**
     * {@inheritDoc}
     *
     * @throws ClassCastException   {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * @implNote This implementation assumes that the ConcurrentMap cannot contain null values and {@code
     * get()} returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */
    default V getOrDefault(Object key, V defaultValue) {
        V v;
        return ((v = get(key)) != null) ? v : defaultValue;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @implSpec The default implementation is equivalent to, for this {@code map}:
     * <pre> {@code
     * for ((Map.Entry<K, V> entry : map.entrySet())
     *     action.accept(entry.getKey(), entry.getValue());
     * }</pre>
     * @implNote The default implementation assumes that {@code IllegalStateException} thrown by {@code
     * getKey()} or {@code getValue()} indicates that the entry has been removed and cannot be processed.
     * Operation continues for subsequent entries.
     * @since 1.8
     */
    default void forEach(BiConsumer<? super K, ? super V> action) {
        Objects.requireNonNull(action);
        for (Map.Entry<K, V> entry : entrySet()) {
            K k;
            V v;
            try {
                k = entry.getKey();
                v = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                continue;
            }
            action.accept(k, v);
        }
    }


    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     * @implSpec <p>The default implementation is equivalent to, for this {@code map}:
     * <pre> {@code
     * for ((Map.Entry<K, V> entry : map.entrySet())
     *     do {
     *        K k = entry.getKey();
     *        V v = entry.getValue();
     *     } while(!replace(k, v, function.apply(k, v)));
     * }</pre>
     *
     * The default implementation may retry these steps when multiple threads attempt updates including
     * potentially calling the function repeatedly for a given key.
     *
     * <p>This implementation assumes that the ConcurrentMap cannot contain null values and {@code get()}
     * returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */
    default void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        Objects.requireNonNull(function);
        final BiConsumer<K, V> action = (k, v) -> {
            while (!replace(k, v, function.apply(k, v))) {
                // v changed or k is gone
                if ((v = get(k)) == null) {
                    // k is no longer in the map.
                    break;
                }
            }
        };

        forEach(action);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @implSpec The default implementation is equivalent to the following steps for this {@code map}, then
     * returning the current value or {@code null} if now absent:
     *
     * <pre> {@code
     * if (map.get(key) == null) {
     *     V newValue = mappingFunction.apply(key);
     *     if (newValue != null)
     *         return map.putIfAbsent(key, newValue);
     * }
     * }</pre>
     *
     * The default implementation may retry these steps when multiple threads attempt updates including
     * potentially calling the mapping function multiple times.
     *
     * <p>This implementation assumes that the ConcurrentMap cannot contain null values and {@code get()}
     * returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */

    default V computeIfAbsent(K key,
                              Function<? super K, ? extends V> mappingFunction) {
        Objects.requireNonNull(mappingFunction);
        V v, newValue;
        return ((v = get(key)) == null &&
                (newValue = mappingFunction.apply(key)) != null &&
                (v = putIfAbsent(key, newValue)) == null) ? newValue : v;
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @implSpec The default implementation is equivalent to performing the following steps for this {@code
     * map}, then returning the current value or {@code null} if now absent. :
     *
     * <pre> {@code
     * if (map.get(key) != null) {
     *     V oldValue = map.get(key);
     *     V newValue = remappingFunction.apply(key, oldValue);
     *     if (newValue != null)
     *         map.replace(key, oldValue, newValue);
     *     else
     *         map.remove(key, oldValue);
     * }
     * }</pre>
     *
     * The default implementation may retry these steps when multiple threads attempt updates including
     * potentially calling the remapping function multiple times.
     *
     * <p>This implementation assumes that the ConcurrentMap cannot contain null values and {@code get()}
     * returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */
    default V computeIfPresent(K key,
                               BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        V oldValue;
        while ((oldValue = get(key)) != null) {
            V newValue = remappingFunction.apply(key, oldValue);
            if (newValue != null) {
                if (replace(key, oldValue, newValue))
                    return newValue;
            } else if (remove(key, oldValue))
                return null;
        }
        return oldValue;
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @implSpec The default implementation is equivalent to performing the following steps for this {@code
     * map}, then returning the current value or {@code null} if absent:
     *
     * <pre> {@code
     * V oldValue = map.get(key);
     * V newValue = remappingFunction.apply(key, oldValue);
     * if (oldValue != null ) {
     *    if (newValue != null)
     *       map.replace(key, oldValue, newValue);
     *    else
     *       map.remove(key, oldValue);
     * } else {
     *    if (newValue != null)
     *       map.putIfAbsent(key, newValue);
     *    else
     *       return null;
     * }
     * }</pre>
     *
     * The default implementation may retry these steps when multiple threads attempt updates including
     * potentially calling the remapping function multiple times.
     *
     * <p>This implementation assumes that the ConcurrentMap cannot contain null values and {@code get()}
     * returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */
    default V compute(K key,
                      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        V oldValue = get(key);
        for (; ; ) {
            V newValue = remappingFunction.apply(key, oldValue);
            if (newValue == null) {
                // delete mapping
                if (oldValue != null || containsKey(key)) {
                    // something to remove
                    if (remove(key, oldValue)) {
                        // removed the old value as expected
                        return null;
                    }

                    // some other value replaced old value. try again.
                    oldValue = get(key);
                } else {
                    // nothing to do. Leave things as they were.
                    return null;
                }
            } else {
                // add or replace old mapping
                if (oldValue != null) {
                    // replace
                    if (replace(key, oldValue, newValue)) {
                        // replaced as expected.
                        return newValue;
                    }

                    // some other value replaced old value. try again.
                    oldValue = get(key);
                } else {
                    // add (replace if oldValue was null)
                    if ((oldValue = putIfAbsent(key, newValue)) == null) {
                        // replaced
                        return newValue;
                    }

                    // some other value replaced old value. try again.
                }
            }
        }
    }


    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @implSpec The default implementation is equivalent to performing the following steps for this {@code
     * map}, then returning the current value or {@code null} if absent:
     *
     * <pre> {@code
     * V oldValue = map.get(key);
     * V newValue = (oldValue == null) ? value :
     *              remappingFunction.apply(oldValue, value);
     * if (newValue == null)
     *     map.remove(key);
     * else
     *     map.put(key, newValue);
     * }</pre>
     *
     * <p>The default implementation may retry these steps when multiple threads attempt updates including
     * potentially calling the remapping function multiple times.
     *
     * <p>This implementation assumes that the ConcurrentMap cannot contain null values and {@code get()}
     * returning null unambiguously means the key is absent. Implementations which support null values
     * <strong>must</strong> override this default implementation.
     * @since 1.8
     */
    default V merge(K key, V value,
                    BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        Objects.requireNonNull(value);
        V oldValue = get(key);
        for (; ; ) {
            if (oldValue != null) {
                V newValue = remappingFunction.apply(oldValue, value);
                if (newValue != null) {
                    if (replace(key, oldValue, newValue))
                        return newValue;
                } else if (remove(key, oldValue)) {
                    return null;
                }
                oldValue = get(key);
            } else {
                if ((oldValue = putIfAbsent(key, value)) == null) {
                    return value;
                }
            }
        }
    }
}
