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

import net.openhft.lang.collection.DirectBitSet;

/**
 * This is only used to store keys and positions, but it could store int/int key/values for another purpose.
 */
interface IntIntMultiMap {

    /**
     * @return gets all the active positions as a bitset
     */
    DirectBitSet getPositions();

    /**
     * Add an entry.  Allow duplicate hashes, but not key/position pairs.
     *
     * @param key   to add
     * @param value to add
     */
    void put(int key, int value);

    /**
     * Remove a key/value pair.
     *
     * @param key   to remove
     * @param value to remove
     * @return whether a match was found.
     */
    boolean remove(int key, int value);

    boolean replace(int key, int oldValue, int newValue);

    /**
     * Used for start a search for a given key
     *
     * @return normalized key value, better to be used in subsequent calls
     */
    int startSearch(int key);

    int getSearchHash();

    /**
     * Used for getting the next position for a given key
     *
     * @return the next position for the last search or negative value
     */
    int nextPos();

    void removePrevPos();

    void replacePrevPos(int newValue);

    void putAfterFailedSearch(int value);

    void clear();

    void forEach(EntryConsumer action);

    static interface EntryConsumer {
        void accept(int key, int value);
    }


}
