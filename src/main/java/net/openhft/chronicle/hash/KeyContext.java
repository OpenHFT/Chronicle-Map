/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.util.ConcurrentModificationException;

/**
 * Common considerations:
 * <ul>
 *     <li>All method throw {@link ConcurrentModificationException} immediately, if called not from
 *     the thread, in which the context was obtained. {@code KeyContext} is strictly
 *     for single-thread access. For single try-with-resources block, actually, because:</li>
 *     <li>All methods throw {@code IllegalStateException} immediately, if called after
 *     {@link #close()} called on this context and returned.</li>
 * </ul>
 *
 * @param <K> key type
 * @deprecated incoherent abstraction. Replaced by {@link HashQueryContext} and {@link HashEntry}
 */
@Deprecated
public interface KeyContext<K> extends AutoCloseable, InterProcessReadWriteUpdateLock {

    /**
     * Returns the key object of this context. The instance might be reused, so you shouldn't
     * save the returned object and use it after the context is closed or the callback (to which
     * this context is provided) return. Might acquire {@link #readLock} before reading the key,
     * if the context is not locked yet.
     *
     * @return the key object of this context
     */
    @NotNull
    K key();

    /**
     * Returns the entry bytes, if the key is present in the hash, otherwise throws
     * {@code IllegalStateException}. Might acquire {@link #readLock} before searching for
     * the entry, if the context is not locked yet.
     *
     * @return the entry bytes
     * @throws IllegalStateException if the hash doesn't {@link #containsKey}
     */
    @NotNull
    Bytes entry();

    /**
     * Returns the offset of the key bytes within the {@link #entry}, if the key is present
     * in the hash, otherwise throws {@code IllegalStateException}. Might acquire {@link #readLock}
     * before searching for the entry, if the context is not locked yet.
     *
     * @return the offset of the key bytes within the {@link #entry}
     * @throws IllegalStateException if the hash doesn't {@link #containsKey}
     */
    long keyOffset();

    /**
     * Returns the size of the key bytes representation. Bytes from {@link #keyOffset} to
     * {@code keyOffset + keySize} are the key bytes in the {@link #entry}.
     *
     * @return the size of the key bytes representation
     */
    long keySize();

    /**
     * Checks if the key is present in the hash. Might acquire {@link #readLock} before searching
     * for the key, if the context is not locked yet.
     *
     * @return {@code true} if the key is present in the hash, {@code false} otherwise
     * @throws IllegalStateException if at least {@link #readLock} is not held
     */
    boolean containsKey();

    /**
     * Removes the key from the hash. Might acquire {@link #updateLock} or {@link #writeLock},
     * if the context is not locked yet. Might upgrade from update to write lock, if update lock
     * is already held.
     *
     * @return {@code true} if the key was actually removed (hash was updated),
     * {@code false} if the key is already absent in the hash
     * @throws IllegalStateException if the context is locked for read, but not for update, and
     * update or write lock is actually required for {@code remove()} in this {@code KeyContext}
     * implementation
     */
    boolean remove();

    /**
     * Closes the context. Among other things, it releases all locks, acquired within the context,
     * that means you shouldn't bother with unlocking and try-finally pattern, if the whole
     * context operation if performed within try-with-resources block: <pre>{@code
     * try (KeyContext c = map.context(key)) {
     *     c.writeLock().lock();
     *     if (random.nextBoolean())
     *         c.remove();
     *     // Note, there is no c.writeLock().unlock(), and it's OK,
     *     // because all locks are released in close()
     * }}</pre>
     */
    @Override
    void close();
}
