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

import net.openhft.chronicle.hash.ExternalHashQueryContext;

/**
 * {@link SetQueryContext} + {@link AutoCloseable}, for external {@link ChronicleSet} queries
 * in <i>try-with-resources</i> blocks.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations}, specified for the queried {@code
 *            ChronicleSet}
 * @see ChronicleSet#queryContext(Object)
 */
public interface ExternalSetQueryContext<K, R>
        extends SetQueryContext<K, R>, ExternalHashQueryContext<K> {
}
