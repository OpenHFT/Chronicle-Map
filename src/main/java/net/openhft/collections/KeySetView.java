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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface KeySetView<K, V> extends Collection<K>, java.io.Serializable, Set<K>, CollectionView<K, V,
        K> {
    /**
     * Returns the default mapped value for additions, or {@code null} if additions are not supported.
     *
     * @return the default mapped value for additions, or {@code null} if not supported
     */
    V getMappedValue();

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    boolean contains(Object o);

    /**
     * Removes the key from this map view, by removing the key (and its corresponding value) from the backing
     * map.  This method does nothing if the key is not in the map.
     *
     * @param o the key to be removed from the backing map
     * @return {@code true} if the backing map contained the specified key
     * @throws NullPointerException if the specified key is null
     */
    boolean remove(Object o);

    /**
     * @return an iterator over the keys of the backing map
     */
    Iterator<K> iterator();

    /**
     * Adds the specified key to this set view by mapping the key to the default mapped value in the backing
     * map, if defined.
     *
     * @param e key to be added
     * @return {@code true} if this set changed as a result of the call
     * @throws NullPointerException          if the specified key is null
     * @throws UnsupportedOperationException if no default mapped value for additions was provided
     */
    boolean add(K e);

    /**
     * Adds all of the elements in the specified collection to this set, as if by calling {@link #add} on each
     * one.
     *
     * @param c the elements to be inserted into this set
     * @return {@code true} if this set changed as a result of the call
     * @throws NullPointerException          if the collection or any of its elements are {@code null}
     * @throws UnsupportedOperationException if no default mapped value for additions was provided
     */
    boolean addAll(Collection<? extends K> c);

    int hashCode();

    boolean equals(Object o);

    Spliterator<K> spliterator();

    void forEach(Consumer<? super K> action);
}
