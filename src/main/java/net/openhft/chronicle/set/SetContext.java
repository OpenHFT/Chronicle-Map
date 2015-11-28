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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.HashContext;

/**
 * Context, in which {@link SetEntry SetEntries} are accessed. {@code SetContext} allows to access
 * {@link SetEntryOperations}, configured for the accessed {@link ChronicleSet}. {@code SetContext}
 * implements {@code SetEntryOperations} by delegation to the configured {@code entryOperations}.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specified for the queried set
 */
public interface SetContext<K, R> extends HashContext<K>, SetEntryOperations<K, R> {

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #set()}.
     */
    @Override
    ChronicleHash<K, ?, ?, ?> hash();

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #hash()}.
     */
    ChronicleSet<K> set();
}
