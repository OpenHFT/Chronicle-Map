/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import net.openhft.chronicle.hash.HashQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * A context of {@link ChronicleSet} operations with <i>individual keys</i>
 * (most: {@code contains()}, {@code add()}, etc., opposed to <i>bulk</i> operations).
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see ChronicleSet#queryContext(Object)
 */
public interface SetQueryContext<K, R> extends HashQueryContext<K>, SetContext<K, R> {

    @Override
    @Nullable
    SetEntry<K> entry();

    @Override
    @Nullable
    SetAbsentEntry<K> absentEntry();
}
