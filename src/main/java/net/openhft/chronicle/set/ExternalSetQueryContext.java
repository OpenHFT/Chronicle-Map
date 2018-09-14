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
