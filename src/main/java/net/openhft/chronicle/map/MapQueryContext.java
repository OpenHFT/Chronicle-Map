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
