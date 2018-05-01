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

import net.openhft.chronicle.hash.HashEntry;

/**
 * A context of a <i>present</i> entry in the {@link ChronicleSet}.
 *
 * @param <K> the set key type
 * @see SetEntryOperations
 * @see SetQueryContext#entry()
 */
public interface SetEntry<K> extends HashEntry<K> {
    @Override
    SetContext<K, ?> context();

    /**
     * Removes the entry from the {@code ChronicleSet}.
     * <p>
     * <p>This method is the default implementation for {@link SetEntryOperations#remove(SetEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
