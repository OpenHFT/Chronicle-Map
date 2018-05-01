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

import net.openhft.chronicle.hash.ChronicleHash;

import java.util.Set;

/**
 * {@code ChronicleSet} provides concurrent access to a <i>Chronicle Map key-value store</i> with
 * zero-sized values from a JVM process, wrapped as an extension of {@link Set} interface.
 *
 * @param <K> the set key type
 * @see net.openhft.chronicle.map.ChronicleMap
 */
public interface ChronicleSet<K>
        extends Set<K>, ChronicleHash<K, SetEntry<K>, SetSegmentContext<K, ?>,
        ExternalSetQueryContext<K, ?>> {

    /**
     * Delegates to {@link ChronicleSetBuilder#of(Class)} for convenience.
     *
     * @param keyClass class of the key type of the {@code ChronicleSet} to create
     * @param <K>      the key type of the {@code ChronicleSet} to create
     * @return a new {@code ChronicleSetBuilder} for the given key class
     */
    static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return ChronicleSetBuilder.of(keyClass);
    }
}
