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

import net.openhft.lang.io.Bytes;

import java.io.Serializable;

/**
 * Contains methods which are called when {@link ChronicleMap} key events occur. Typical use cases:
 * <ul>
 *     <li>Map data backup / replication.</li>
 *     <li>Logging, monitoring, debugging.</li>
 * </ul>
 *
 * <p>This is an adapter class - all methods have default implementations as no-ops. Extend this
 * class and override only methods corresponding the events you are interested in.
 *
 * <p>To configure {@code MapEventListener} for {@code ChronicleMap}, use
 * {@link AbstractChronicleMapBuilder#eventListener(MapEventListener)} method.
 *
 * <p>{@link MapEventListeners} uninstantiable class contains several logging implementations and
 * the default {@linkplain MapEventListeners#nop() no-op implementation}.
 *
 * <p>All these calls are synchronous while a {@code ChronicleMap} lock is held so make them
 * as quick as possible.
 *
 * @param <K> key type of the maps, trackable by this event listener
 * @param <V> value type of the maps, trackable by this event listener
 * @param <M> {@code ChronicleMap} subtype, trackable by this event listener
 * @see AbstractChronicleMapBuilder#eventListener(MapEventListener)
 * @see MapEventListeners
 */
public abstract class MapEventListener<K, V, M extends ChronicleMap<K, V>>
        implements Serializable {
    private static final long serialVersionUID = 0L;

    /**
     * This method is called if the key is found in the map during {@link ChronicleMap#get get},
     * {@link ChronicleMap#getUsing getUsing} or {@link ChronicleMap#acquireUsing acquireUsing}
     * method call.
     *
     * @param map           the accessed map
     * @param entry         bytes of the entry
     * @param metaDataBytes length of meta data for this map
     * @param key           the key looked up
     * @param value         the value found for the key
     */
    public void onGetFound(M map, Bytes entry, int metaDataBytes, K key, V value) {
        // do nothing
    }

    void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value,
               long pos, SharedSegment segment) {
        onPut(map, entry, metaDataBytes, added, key, value);
    }

    /**
     * This method is call whenever a new value is put for the key in the map during calls of such
     * methods as {@link ChronicleMap#put put}, {@link ChronicleMap#putIfAbsent putIfAbsent},
     * {@link ChronicleMap#replace(Object, Object, Object) replace}, etc. When a new value is
     * {@linkplain AbstractChronicleMapBuilder#defaultValue(Object) default} for the map or obtained during
     * {@link ChronicleMap#acquireUsing acquireUsing} call is put for the key, this method is called
     * as well.
     *
     * <p>This method is called when put is already happened.
     *
     * @param map           the accessed map
     * @param entry         bytes of the entry (with the value already written to)
     * @param metaDataBytes length of meta data for this map
     * @param added         {@code true} is the key was absent in the map before current
     *                      {@code ChronicleMap} method call, led to putting a value and inherently
     *                      calling this listener method
     * @param key           the key the value is put for
     * @param value         the value which was associated with the key
     */
    public void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value) {
        // do nothing
    }

    void onRemove(M map, Bytes entry, int metaDataBytes, K key, V value,
                  long pos, SharedSegment segment) {
        onRemove(map, entry, metaDataBytes, key, value);
    }

    /**
     * This is called when an entry is removed. Misses are not notified.
     *
     * @param map           accessed
     * @param entry         removed
     * @param metaDataBytes length of meta data
     * @param key           removed
     * @param value         removed
     */
    public void onRemove(M map, Bytes entry, int metaDataBytes, K key, V value) {
        // do nothing
    }

    void onRelocation(long pos, SharedSegment segment) {
        // do nothing
    }
}
