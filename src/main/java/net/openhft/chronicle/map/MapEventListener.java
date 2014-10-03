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

import net.openhft.lang.io.Bytes;

import java.io.Serializable;

/**
 * This event listener is called when key events occur.
 * <p>All these calls are synchronous while a lock is held so make them as quick as possible</p>
 * <p/>
 * TODO specify more clearly in which cases methods are called.
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
     * This method is called if a key/value is put in the map.
     *
     * @param map           accessed
     * @param entry         added/modified
     * @param metaDataBytes length of the meta data
     * @param added         if this is a new entry
     * @param key           looked up
     * @param value         set for key
     */
    public void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value) {
        // do nothing
    }

    void onRemove(M map, Bytes entry, int metaDataBytes, K key, V value,
                  int pos, SharedSegment segment) {
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

    void onRelocation(int pos, SharedSegment segment) {
        // do nothing
    }


}
