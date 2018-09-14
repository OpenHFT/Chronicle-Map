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
