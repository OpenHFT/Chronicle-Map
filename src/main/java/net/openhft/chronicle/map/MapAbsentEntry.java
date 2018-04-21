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
import net.openhft.chronicle.hash.HashAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleMap}.
 *
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 * @see MapEntryOperations
 * @see MapQueryContext#absentEntry()
 */
public interface MapAbsentEntry<K, V> extends HashAbsentEntry<K> {

    @Override
    @NotNull
    MapContext<K, V, ?> context();

    /**
     * Inserts the new entry into the map, of {@linkplain #absentKey() the key} and the given {@code
     * value}.
     * <p>
     * <p>This method is the default implementation for {@link MapEntryOperations#insert(
     *MapAbsentEntry, Data)}, which might be customized over the default.
     *
     * @param value the value to insert into the map along with {@link #absentKey() the key}
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @see MapEntryOperations#insert(MapAbsentEntry, Data)
     */
    void doInsert(Data<V> value);

    /**
     * Returns the <i>default</i> (or <i>nil</i>) value, that should be inserted into the map in
     * this context. This is primarily used in {@link ChronicleMap#acquireUsing} operation
     * implementation, i. e. {@link MapMethods#acquireUsing}.
     * <p>
     * <p>This method if the default implementation for {@link
     * DefaultValueProvider#defaultValue(MapAbsentEntry)},
     * which might be customized over the default.
     *
     * @see DefaultValueProvider#defaultValue(MapAbsentEntry)
     */
    @NotNull
    Data<V> defaultValue();
}
