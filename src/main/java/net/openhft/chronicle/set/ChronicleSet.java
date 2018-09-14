/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
