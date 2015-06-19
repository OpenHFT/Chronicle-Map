/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
