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
 * Result of {@link ChronicleMap#update(Object, Object)} operation.
 */
public enum UpdateResult {
    /**
     * The new entry was put into the Chroncile Map.
     */
    INSERT,

    /**
     * The existing entry was updated, the previous value differs from the value was put.
     */
    UPDATE,

    /**
     * The exiting entry was checked, it maps to the same value, as was going to be put.
     */
    UNCHANGED
}
