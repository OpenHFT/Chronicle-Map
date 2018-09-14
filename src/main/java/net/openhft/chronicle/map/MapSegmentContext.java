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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.HashSegmentContext;

/**
 * Context of {@link ChronicleMap}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleMap}
 * @param <V> the value type of accessed {@code ChronicleMap}
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 * @see ChronicleMap#segmentContext(int)
 */
public interface MapSegmentContext<K, V, R>
        extends HashSegmentContext<K, MapEntry<K, V>>, MapContext<K, V, R> {
}
