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

import net.openhft.lang.collection.DirectBitSet;

/**
 * This is only used to store keys and positions, but it could store int/int key/values for another purpose.
 */
interface MultiMap {

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
    void put(long key, long value);

    void putPosition(long value);

    /**
     * Remove a key/value pair.
     *
     * @param key   to remove
     * @param value to remove
     * @throws RuntimeException if key-value pair is absent in the multi map
     */
    void remove(long key, long value);

    void removePosition(long value);

    void replace(long key, long oldValue, long newValue);

    /**
     * Used for start a search for a given key
     */
    void startSearch(long key, SearchState searchStateToReuse);

    /**
     * Used for getting the next position for a given key
     *
     * @return the next position for the last search or negative value
     */
    long nextPos(SearchState searchState);

    void removePrevPos(SearchState searchState);

    void replacePrevPos(SearchState searchState, long newValue, boolean oldValueInPositions);

    void putAfterFailedSearch(SearchState searchState, long value);

    void clear();

    void forEach(EntryConsumer action);

    static interface EntryConsumer {
        void accept(long key, long value);
    }
}
