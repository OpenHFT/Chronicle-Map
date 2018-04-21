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

import net.openhft.chronicle.hash.ExternalHashQueryContext;

/**
 * {@link MapQueryContext} + {@link AutoCloseable}, for external {@link ChronicleMap} queries
 * in <i>try-with-resources</i> blocks.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations}, {@link
 *            ChronicleMapBuilder#entryOperations(MapEntryOperations) specified} for the queried {@code
 *            ChronicleMap}
 * @see ChronicleMap#queryContext(Object)
 */
public interface ExternalMapQueryContext<K, V, R>
        extends MapQueryContext<K, V, R>, ExternalHashQueryContext<K> {
}
