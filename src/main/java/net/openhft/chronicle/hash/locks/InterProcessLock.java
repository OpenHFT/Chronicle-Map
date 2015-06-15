/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.locks;

import net.openhft.chronicle.hash.ChronicleHash;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Abstracts inter-process lock of {@link ChronicleHash} data: segments, entries.
 *
 * <p>This lock is not reentrant, but kind of "saturating": multiple {@link #lock()} calls have
 * the same effect as a single call. Likewise {@link #unlock()} -- multiple unlocks, or "unlocking"
 * when the lock isn't actually held, has no negative effects.
 *
 * <p>Once a lock object obtained, it <i>shouldn't be stored in a field and accessed from multiple
 * threads</i>, instead of that, lock objects should be obtained in each thread separately, using
 * the same call chain. This is because since the lock is inter-process, it anyway keeps the
 * synchronization state in shared memory, but restricting on-heap "view" of shared lock to a single
 * thread is beneficial form performance point-of-view, e. g. fields of the on-heap
 * {@code InterProcessLock} object shouldn't be {@code volatile}.
 *
 * <p>Locks is inter-process, hence it cannot afford to wait for acquisition infinitely, because
 * it would be too dead-lock prone. {@link #lock()} throws {@code RuntimeException} after some
 * finite time.
 *
 * @implNote Inter-process lock is unfair.
 *
 * @see InterProcessReadWriteUpdateLock
 */
public interface InterProcessLock extends Lock {

    /**
     * Checks if this lock is held by current thread.
     */
    boolean isHeldByCurrentThread();

    /**
     * Acquires the lock, if this lock (or stronger-level lock, in the context of {@link
     * InterProcessReadWriteUpdateLock}) is already held by current thread, this call returns
     * immediately.
     *
     * <p>If the lock is not available then the current thread enter a busy loop. After some
     * specified threshold, the thread <i>might</i> be disabled for thread scheduling purposes
     * and lies dormant until the lock has been acquired. After another specified threshold
     * {@link RuntimeException} is thrown.
     *
     * @throws IllegalInterProcessLockStateException if this method call observes illegal lock
     * state, or some lock limitations reached (e. g. maximum read lock holders)
     * @throws RuntimeException if fails to acquire a lock for some finite time
     */
    @Override
    void lock();

    /**
     * Acquires the lock only if it is free at the time of invocation, also if the lock is already
     * held by the current thread, this call immediately returns {@code true}.
     *
     * <p>Acquires the lock if it is available and returns immediately
     * with the value {@code true}.
     * If the lock is not available then this method will return
     * immediately with the value {@code false}.
     *
     * <p>Example usage: <pre>{@code
     * try (ExternalMapQueryContext<K, V, ?> q = map.queryContext(key)) {
     *     if (q.updateLock().tryLock()) {
     *         // highly-probable branch
     *         if (q.entry() != null) {
     *             // upgrade to write lock
     *             q.writeLock().lock();
     *             q.remove(q.entry());
     *         } else {
     *             // ...
     *         }
     *     } else {
     *         // if failed to acquire the update lock without waiting, go acquire the write lock
     *         // right away, because probability that we will need to upgrade to write lock anyway
     *         // is high.
     *         q.writeLock().lock();
     *         if (q.entry() != null) {
     *             q.remove(q.entry());
     *         } else {
     *             // ...
     *         }
     *     }
     * }}</pre>
     *
     * @return {@code true} if the lock was acquired and {@code false} otherwise
     * @throws IllegalInterProcessLockStateException if this method call observes illegal lock
     * state, or some lock limitations reached (e. g. maximum read lock holders)
     */
    @Override
    boolean tryLock();

    /**
     * Releases the lock, if the lock is not held by the current thread, returns immediately.
     *
     * @throws IllegalInterProcessLockStateException if this method call observes illegal lock
     * state
     */
    @Override
    void unlock();

    /**
     * Conditions are not supported by inter-process locks, always throws {@code
     * UnsupportedOperationException}.
     *
     * @return nothing, always throws an exception
     * @throws UnsupportedOperationException always
     */
    @NotNull
    @Override
    default Condition newCondition() {
        throw new UnsupportedOperationException(
                "Conditions are not supported by inter-process locks");
    }
}
