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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@link ChronicleMap}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see MapEntryOperations
 * @see MapQueryContext#entry()
 */
public interface MapEntry<K, V> extends HashEntry<K> {
    @Override
    @NotNull
    MapContext<K, V, ?> context();

    /**
     * Returns the entry value.
     */
    @NotNull
    Data<V> value();

    /**
     * Replaces the entry's value with the given {@code newValue}.
     * <p>
     * <p>This method is the default implementation for {@link MapEntryOperations#replaceValue(
     *MapEntry, Data)}, which might be customized over the default.
     *
     * @param newValue the value to be put into the map instead of the {@linkplain #value() current
     *                 value}
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     *                               operation are not met
     */
    void doReplaceValue(Data<V> newValue);

    /**
     * Removes the entry from the map.
     * <p>
     * <p>This method is the default implementation for {@link MapEntryOperations#remove(MapEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
