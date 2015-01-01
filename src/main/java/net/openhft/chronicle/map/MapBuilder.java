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

import java.util.HashMap;
import java.util.Map;

/**
 * Common configurations of {@link ChronicleMapBuilder} and
 * {@link ChronicleMapStatelessClientBuilder}.
 *
 * @param <C> concrete builder class
 */
public interface MapBuilder<C extends MapBuilder<C>> {

    /**
     * Configures if the maps created by this {@code MapBuilder} should return {@code null}
     * instead of previous mapped values on {@link ChronicleMap#put(Object, Object)
     * ChornicleMap.put(key, value)} calls.
     *
     * <p>{@link Map#put(Object, Object) Map.put()} returns the previous value, functionality
     * which is rarely used but fairly cheap for simple in-process, on-heap implementations like
     * {@link HashMap}. But an off-heap collection has to create a new object and deserialize
     * the data from off-heap memory. A collection hiding remote queries over the network should
     * send the value back in addition to that. It's expensive for something you probably don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract and
     * returns the previous mapped value on {@code put()} calls.
     *
     * @param putReturnsNull {@code true} if you want {@link ChronicleMap#put(Object, Object)
     *                       ChronicleMap.put()} to not return the value that was replaced but
     *                       instead return {@code null}
     * @return {@code MapBuilder} with this configuration applied
     * @see #removeReturnsNull(boolean)
     */
    C putReturnsNull(boolean putReturnsNull);


    /**
     * Configures if the maps created by this {@code MapBuilder} should return {@code null}
     * instead of the last mapped value on {@link ChronicleMap#remove(Object)
     * ChronicleMap.remove(key)} calls.
     *
     * <p>{@link Map#remove(Object) Map.remove()} returns the previous value, functionality which is
     * rarely used but fairly cheap for simple in-process, on-heap implementations like {@link
     * HashMap}. But an off-heap collection has to create a new object and deserialize the data
     * from off-heap memory. A collection hiding remote queries over the network should send
     * the value back in addition to that. It's expensive for something you probably don't use.
     *
     * <p>By default, of cause, {@code ChronicleMap} conforms the general {@code Map} contract and
     * returns the mapped value on {@code remove()} calls.
     *
     * @param removeReturnsNull {@code true} if you want {@link ChronicleMap#remove(Object)
     *                          ChronicleMap.remove()} to not return the value of the removed entry
     *                          but instead return {@code null}
     * @return {@code MapBuilder} with this configuration applied
     * @see #putReturnsNull(boolean)
     */
    C removeReturnsNull(boolean removeReturnsNull);
}
