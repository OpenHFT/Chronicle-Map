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
