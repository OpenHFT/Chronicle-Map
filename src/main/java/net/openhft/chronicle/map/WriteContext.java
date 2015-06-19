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

/**
 * @author Rob Austin.
 */
public interface WriteContext<K, V> extends Context<K, V> {

    /**
     * Prevents the entry being put back back into the map when the Context is closed
     *
     * @throws java.lang.IllegalArgumentException if used in a native context
     */
    void dontPutOnClose();

    /**
     * this is similar by more efficient than calling map.remove("key") as the entry is already
     * available to the WriteContext
     */
    void removeEntry();

    /**
     * @return true if the entry was previously present
     */
    boolean created();

}
