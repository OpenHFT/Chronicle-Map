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

package net.openhft.chronicle.hash;

/**
 * {@link HashQueryContext} + {@link AutoCloseable}, for external {@link ChronicleHash} queries
 * in <i>try-with-resources</i> blocks.
 *
 * @param <K> the hash key type
 * @see ChronicleHash#queryContext(Object)
 */
public interface ExternalHashQueryContext<K> extends HashQueryContext<K>, AutoCloseable {

    /**
     * Closes the query context, automatically releases all locks and disposes all resources,
     * acquired during the query operation. I. e. you shouldn't release locks manually in the end
     * of try-with-resources statement: <pre>{@code
     * try (ExternalHashQueryContext<K> q = hash.queryContext(key))
     *     q.writeLock().lock();
     *     // ...make a query under exclusive lock
     *     // NOT NEEDED - q.readLock().unlock();
     * }}</pre>
     */
    @Override
    void close();
}
