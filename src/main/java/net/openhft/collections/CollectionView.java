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

/**
 * @author Rob Austin.
 */
public interface CollectionView<K, V, E> {

    AbstractVanillaSharedMap<K, V> getMap();

    /**
     * Removes all of the elements from this view, by removing all the mappings from the map backing this
     * view.
     */
    void clear();

    int size();


    boolean isEmpty();

    // implementations below rely on concrete classes supplying these
    // abstract methods

    /**
     * Returns an iterator over the elements in this collection.
     *
     * <p>The returned iterator is <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return an iterator over the elements in this collection
     */
    Iterator<E> iterator();

    boolean contains(Object o);

    boolean remove(Object o);

    static final String oomeMsg = "Required array size too large";

    Object[] toArray();

    @SuppressWarnings("unchecked")
    <T> T[] toArray(T[] a);

    /**
     * Returns a string representation of this collection. The string representation consists of the string
     * representations of the collection's elements in the order they are returned by its iterator, enclosed
     * in square brackets ({@code "[]"}). Adjacent elements are separated by the characters {@code ", "}
     * (comma and space).  Elements are converted to strings as by {@link String#valueOf(Object)}.
     *
     * @return a string representation of this collection
     */
    String toString();

    boolean containsAll(Collection<?> c);

    boolean removeAll(Collection<?> c);

    boolean retainAll(Collection<?> c);
}
