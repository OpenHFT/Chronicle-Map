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

package net.openhft.chronicle.hash.locks;

import net.openhft.chronicle.hash.ChronicleHash;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * An inter-process lock, used to control access to some shared off-heap resources of {@link
 * ChronicleHash} instances.
 * <p>
 * <p>This lock is not reentrant, but kind of "saturating": multiple {@link #lock()} calls have
 * the same effect as a single call. Likewise {@link #unlock()} -- multiple unlocks, or unlocking
 * when the lock isn't actually held, has no negative effects.
 * <p>
 * <p>Once a lock object obtained, it <i>shouldn't be stored in a field and accessed from multiple
 * threads</i>, instead of that, lock objects should be obtained in each thread separately, using
 * the same call chain. This is because since the lock is inter-process, it anyway keeps it's
 * synchronization state in shared off-heap memory, but restricting on-heap "view" of shared lock
 * to a single thread is beneficial form performance point-of-view, e. g. fields of the on-heap
 * {@code InterProcessLock} object shouldn't be {@code volatile}.
 * <p>
 * <p>Lock is inter-process, hence it cannot afford to wait for acquisition infinitely, because
 * it would be too dead-lock prone. {@link #lock()} throws {@code RuntimeException} after some
 * implementation-defined time spent in waiting for the lock acquisition.
 * <p>
 * <p>{@code InterProcessLock} supports interruption of lock acquisition (in {@link
 * #lockInterruptibly()} and {@link #tryLock(long, TimeUnit)} methods).
 *
 * @implNote Inter-process lock is unfair.
 * @see InterProcessReadWriteUpdateLock
 */
public interface InterProcessLock extends Lock {

    /**
     * Checks if this lock is held by current thread.
     */
    boolean isHeldByCurrentThread();

    /**
     * Acquires the lock. If this lock (or a stronger-level lock, in the context of {@link
     * InterProcessReadWriteUpdateLock}) is already held by the current thread, this call returns
     * immediately.
     * <p>
     * <p>If the lock is not available then the current thread enters a busy loop. After some
     * threshold time spent in a busy loop, the thread <i>might</i> be disabled for thread
     * scheduling purposes and lay dormant until the lock has been acquired. After some
     * implementation-defined time spent in waiting for the lock acquisition,
     * {@link InterProcessDeadLockException} is thrown.
     *
     * @throws IllegalMonitorStateException  if this method call observes illegal lock state, or some
     *                                       lock limitations reached (e. g. maximum read lock holders)
     * @throws InterProcessDeadLockException if fails to acquire a lock for some finite time
     */
    @Override
    void lock();

    /**
     * Acquires the lock unless the current thread is {@linkplain Thread#interrupt interrupted}. If
     * the current thread is not interrupted, and this lock (or a stronger-level lock, in the
     * context of {@link InterProcessReadWriteUpdateLock}) is already held by the current thread,
     * this call returns immediately.
     * <p>
     * <p>If the lock is not available then the current thread enters a busy loop, and after some
     * threshold time spend in a busy loop, the thread <i>might</i> be disabled for thread
     * scheduling purposes and lay dormant until one of three things happens:
     * <p>
     * <ul>
     * <li>The lock is acquired by the current thread, then {@code lockInterruptibly()} successfully
     * returns.
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread, then
     * {@link InterruptedException} is thrown and the current thread's interrupted status is
     * cleared.
     * <li>Some implementation-defined time is spent in waiting for the lock acquisition, then
     * {@link InterProcessDeadLockException} is thrown.
     * </ul>
     *
     * @throws InterruptedException          if the current thread is interrupted while acquiring the lock
     * @throws IllegalMonitorStateException  if this method call observes illegal lock state, or some
     *                                       lock limitations reached (e. g. maximum read lock holders)
     * @throws InterProcessDeadLockException if fails to acquire a lock for some finite time
     */
    @Override
    void lockInterruptibly() throws InterruptedException;

    /**
     * Acquires the lock only if it is free at the time of invocation, also if the lock is already
     * held by the current thread, this call immediately returns {@code true}.
     * <p>
     * <p>Acquires the lock if it is available and returns immediately
     * with the value {@code true}.
     * If the lock is not available then this method will return
     * immediately with the value {@code false}.
     *
     * @return {@code true} if the lock was acquired and {@code false} otherwise
     * @throws IllegalMonitorStateException if this method call observes illegal lock state, or some
     *                                      lock limitations reached (e. g. maximum read lock holders)
     * @apiNote Example usage: <pre>{@code
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
     */
    @Override
    boolean tryLock();

    /**
     * Releases the lock (and all stronger-level lock, in the context of {@link
     * InterProcessReadWriteUpdateLock}, if the lock is not held by the current thread, returns
     * immediately.
     *
     * @throws IllegalMonitorStateException if this method call observes illegal lock state
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
