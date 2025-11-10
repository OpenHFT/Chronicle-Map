//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
