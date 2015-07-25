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

import net.openhft.chronicle.hash.HashSegmentContext;

/**
 * Context of {@link ChronicleSet}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleSet}
 * @param <R> the return type of {@link SetEntryOperations} specified for the queried set
 * @see ChronicleSet#segmentContext(int)
 */
public interface SetSegmentContext<K, R>
        extends HashSegmentContext<K, SetEntry<K>>, SetContext<K, R> {
}
