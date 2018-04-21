/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.set;

import org.jetbrains.annotations.NotNull;

/**
 * SPI interface fro customizing "low-level" modification operations on {@link ChronicleSet}
 * entries.
 *
 * @param <K> the set key type
 * @param <R> methods return type, used for communication between lower- and higher-level SPI
 * @see ChronicleSetBuilder#entryOperations(SetEntryOperations)
 */
public interface SetEntryOperations<K, R> {

    /**
     * Removes the given entry from the set.
     *
     * @param entry the entry to remove
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     * @throws RuntimeException      if removal was unconditionally unsuccessful due to any reason
     * @implNote default implementation calls {@link SetEntry#doRemove()} on the given entry and
     * returns {@code null}.
     */
    default R remove(@NotNull SetEntry<K> entry) {
        entry.doRemove();
        return null;
    }

    /**
     * Inserts the new entry into the set, of {@link SetAbsentEntry#absentKey() the key} from
     * the given insertion context (<code>absentEntry</code>).
     *
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @throws RuntimeException      if insertion was unconditionally unsuccessful due to any reason
     * @implNote default implementation calls {@link SetAbsentEntry#doInsert()} and returns
     * {@code null}.
     */
    default R insert(@NotNull SetAbsentEntry<K> absentEntry) {
        absentEntry.doInsert();
        return null;
    }
}
