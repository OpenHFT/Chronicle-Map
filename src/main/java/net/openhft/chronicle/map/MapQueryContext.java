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

import net.openhft.chronicle.hash.HashQueryContext;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.Nullable;

/**
 * A context of {@link ChronicleMap} operations with <i>individual keys</i>
 * (like during {@code get()}, {@code put()}, etc., opposed to <i>bulk</i> operations).
 * This is the main context type of {@link MapMethods} and {@link MapRemoteOperations}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specialized for the queried map
 * @see ChronicleMap#queryContext(Object)
 */
public interface MapQueryContext<K, V, R> extends HashQueryContext<K>, MapContext<K, V, R> {

    @Override
    @Nullable
    MapEntry<K, V> entry();

    @Override
    @Nullable
    MapAbsentEntry<K, V> absentEntry();
}
