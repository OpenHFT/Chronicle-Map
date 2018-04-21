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

package net.openhft.chronicle.hash.locks;

import net.openhft.chronicle.hash.ExternalHashQueryContext;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Tri-level lock, for efficient handling of concurrent accesses, that require different privileges.
 * For grasping multi-level lock concept and basic semantic principles, please read
 * {@link ReadWriteLock} documentation.
 * <p>
 * <p>{@linkplain #readLock() Read lock} is for "readers". Multiple threads might hold read lock
 * simultaneously. Read lock held by any thread prevents write lock from being acquired.
 * <p>
 * <p>{@linkplain #updateLock() Update lock} is for writers, who must access the resource
 * exclusively with each other, but write safely for concurrent readers. Only one thread could hold
 * update or write lock at the same time, but update lock doesn't prevent concurrent readers
 * to come.
 * <p>
 * <p>{@linkplain #writeLock() Write lock} is for writers, which need total exclusiveness. When
 * write lock is held, nobody may access the resource at the same time.
 * <p>
 * <p>Same as {@link InterProcessLock} itself, the {@code InterProcessReadWriteUpdateLock} as a
 * whole is <i>saturating</i>: when stronger-level lock is acquired, all weaker-level locks
 * are thought to be acquired as well ("saturated"). Unlocking a weaker-level lock "desaturates"
 * it with stronger-level locks only. This means that try-finally pattern looks unusual: <pre>{@code
 * l.writeLock().lock(); // acquires all three level locks
 * try {
 *     // do something
 * } finally {
 *     l.readLock().unlock();
 *     // l.writeLock().unlock(); - INCORRECT, because "desaturates" only write-level,
 *     // leaving read lock and update lock held
 * }}</pre>
 * However, if you use {@code InterProcessReadWriteUpdateLock} as a part of {@link
 * ExternalHashQueryContext} query, you shouldn't keep this peculiarity in mind, because {@link
 * ExternalHashQueryContext#close()} handles unlocking for you.
 * <p>
 * <p>Any downgrades ("desaturations") are possible, but only update -> write upgrade is allowed.
 * This is so to prevent dead locks: for example, imagine, that two threads came and acquired read
 * locks, and then both try to upgrade to write lock, blocking each other indefinitely.
 * <p>
 * <p>This interface is based on <a href="https://code.google.com/p/concurrent-locks/">this
 * work</a>, might be useful to read to understand read-write-update lock idea, and how to use
 * this concept.
 * <p>
 * <p>Same as {@code InterProcessLock}, instances of this interface are for single-thread use
 * only. Views of the same inter-process lock should be obtained in different threads independently.
 * See {@link InterProcessLock} documentation for rationalization of this.
 *
 * @see InterProcessLock
 * @see ReadWriteLock
 */
public interface InterProcessReadWriteUpdateLock extends ReadWriteLock {

    /**
     * Returns the read-level lock. Calling {@link InterProcessLock#unlock() unlock()} on this
     * lock also releases the update and write locks, if held, as well.
     *
     * @return the read level lock
     */
    @NotNull
    @Override
    InterProcessLock readLock();

    /**
     * Returns the update-level lock. Calling {@link InterProcessLock#lock() lock()} or other
     * locking methods ({@code tryLock()}, etc.) on this lock acquires the read lock, as well.
     * Calling {@link InterProcessLock#unlock() unlock()} on this lock also releases the write lock.
     * <p>
     * <p>Any attempt to acquire this lock (including {@code tryLock()}, if the read lock is already
     * held by the current thread, but the update lock is not yet, is prevented by throwing {@link
     * IllegalMonitorStateException}.
     *
     * @return the update-level lock
     */
    @NotNull
    InterProcessLock updateLock();

    /**
     * Returns the write-level lock. Calling {@link InterProcessLock#lock() lock()} or other
     * locking methods ({@code tryLock()}, etc.) on this lock acquires the read and update locks,
     * as well.
     * <p>
     * <p>Any attempt to acquire this lock (including {@code tryLock()}, if the read lock is already
     * held by the current thread, but the update and write locks are not yet, is prevented by
     * throwing {@link IllegalMonitorStateException}.
     *
     * @return the update-level lock
     */
    @NotNull
    @Override
    InterProcessLock writeLock();
}
