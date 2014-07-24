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

package net.openhft.collections;

import java.util.Map;
import java.util.function.*;

/**
 * @author Rob Austin.
 */
public interface ConcurrentMapLamdadSupport<K, V> {


    /**
     * Returns a {@link java.util.Set} view of the keys contained in this map. The set is backed by the map,
     * so changes to the map are reflected in the set, and vice-versa. The set supports element removal, which
     * removes the corresponding mapping from this map, via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear} operations.  It does not support the {@code
     * add} or {@code addAll} operations.
     *
     * <p>The view's iterators and spliterators are <a href="package-summary.html#Weakly"><i>weakly
     * consistent</i></a>.
     *
     * <p>The view's {@code spliterator} reports {@link java.util.Spliterator#CONCURRENT}, {@link
     * java.util.Spliterator#DISTINCT}, and {@link java.util.Spliterator#NONNULL}.
     *
     * @return the set view
     */
    AbstractVanillaSharedMap.KeySetView<K, V> keySet();


    long mappingCount();

    net.openhft.collections.KeySetView<K, V> keySet(V mappedValue);

    void forEach(long parallelismThreshold,
                 BiConsumer<? super K, ? super V> action);

    <U> void forEach(long parallelismThreshold,
                     BiFunction<? super K, ? super V, ? extends U> transformer,
                     Consumer<? super U> action);

    <U> U search(long parallelismThreshold,
                 BiFunction<? super K, ? super V, ? extends U> searchFunction);

    <U> U reduce(long parallelismThreshold,
                 BiFunction<? super K, ? super V, ? extends U> transformer,
                 BiFunction<? super U, ? super U, ? extends U> reducer);

    double reduceToDouble(long parallelismThreshold,
                          ToDoubleBiFunction<? super K, ? super V> transformer,
                          double basis,
                          DoubleBinaryOperator reducer);

    long reduceToLong(long parallelismThreshold,
                      ToLongBiFunction<? super K, ? super V> transformer,
                      long basis,
                      LongBinaryOperator reducer);

    int reduceToInt(long parallelismThreshold,
                    ToIntBiFunction<? super K, ? super V> transformer,
                    int basis,
                    IntBinaryOperator reducer);


    void forEachKey(long parallelismThreshold,
                    Consumer<? super K> action);

    <U> void forEachKey(long parallelismThreshold,
                        Function<? super K, ? extends U> transformer,
                        Consumer<? super U> action);

    <U> U searchKeys(long parallelismThreshold,
                     Function<? super K, ? extends U> searchFunction);

    K reduceKeys(long parallelismThreshold,
                 BiFunction<? super K, ? super K, ? extends K> reducer);

    <U> U reduceKeys(long parallelismThreshold,
                     Function<? super K, ? extends U> transformer,
                     BiFunction<? super U, ? super U, ? extends U> reducer);

    double reduceKeysToDouble(long parallelismThreshold,
                              ToDoubleFunction<? super K> transformer,
                              double basis,
                              DoubleBinaryOperator reducer);

    long reduceKeysToLong(long parallelismThreshold,
                          ToLongFunction<? super K> transformer,
                          long basis,
                          LongBinaryOperator reducer);

    int reduceKeysToInt(long parallelismThreshold,
                        ToIntFunction<? super K> transformer,
                        int basis,
                        IntBinaryOperator reducer);

    void forEachValue(long parallelismThreshold,
                      Consumer<? super V> action);

    <U> void forEachValue(long parallelismThreshold,
                          Function<? super V, ? extends U> transformer,
                          Consumer<? super U> action);

    <U> U searchValues(long parallelismThreshold,
                       Function<? super V, ? extends U> searchFunction);

    V reduceValues(long parallelismThreshold,
                   BiFunction<? super V, ? super V, ? extends V> reducer);

    <U> U reduceValues(long parallelismThreshold,
                       Function<? super V, ? extends U> transformer,
                       BiFunction<? super U, ? super U, ? extends U> reducer);

    double reduceValuesToDouble(long parallelismThreshold,
                                ToDoubleFunction<? super V> transformer,
                                double basis,
                                DoubleBinaryOperator reducer);

    long reduceValuesToLong(long parallelismThreshold,
                            ToLongFunction<? super V> transformer,
                            long basis,
                            LongBinaryOperator reducer);

    int reduceValuesToInt(long parallelismThreshold,
                          ToIntFunction<? super V> transformer,
                          int basis,
                          IntBinaryOperator reducer);

    void forEachEntry(long parallelismThreshold,
                      Consumer<? super Map.Entry<K, V>> action);

    <U> void forEachEntry(long parallelismThreshold,
                          Function<Map.Entry<K, V>, ? extends U> transformer,
                          Consumer<? super U> action);

    <U> U searchEntries(long parallelismThreshold,
                        Function<Map.Entry<K, V>, ? extends U> searchFunction);

    Map.Entry<K, V> reduceEntries(long parallelismThreshold,
                                  BiFunction<Map.Entry<K, V>, Map.Entry<K, V>, ? extends Map.Entry<K, V>> reducer);

    <U> U reduceEntries(long parallelismThreshold,
                        Function<Map.Entry<K, V>, ? extends U> transformer,
                        BiFunction<? super U, ? super U, ? extends U> reducer);

    double reduceEntriesToDouble(long parallelismThreshold,
                                 ToDoubleFunction<Map.Entry<K, V>> transformer,
                                 double basis,
                                 DoubleBinaryOperator reducer);

    long reduceEntriesToLong(long parallelismThreshold,
                             ToLongFunction<Map.Entry<K, V>> transformer,
                             long basis,
                             LongBinaryOperator reducer);

    int reduceEntriesToInt(long parallelismThreshold,
                           ToIntFunction<Map.Entry<K, V>> transformer,
                           int basis,
                           IntBinaryOperator reducer);
}
