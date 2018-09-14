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

import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.chronicle.map.MapEntryOperations;
import net.openhft.chronicle.map.MapMethods;
import net.openhft.chronicle.map.MapQueryContext;
import net.openhft.chronicle.set.SetEntryOperations;
import net.openhft.chronicle.set.SetQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of {@link ChronicleHash} operations with <i>individual keys</i>.
 * <p>
 * <p>This context provides access to {@link InterProcessReadWriteUpdateLock}, governing access
 * to the entry. Your have no chance to perform any actions under race with other threads, because
 * all required locks are acquired automatically on each operation, you should deal with locks
 * manually in the following cases:
 * <ul>
 * <li>You should perform some read operation, and then write. On first operation, the context
 * will automatically acquire the read lock; on the second, it will try to acquire the update
 * or write lock, but it will fail with {@link IllegalMonitorStateException}, because this is
 * dead lock prone. (See {@link InterProcessReadWriteUpdateLock} documentation for explanation.
 * So, such context code is incorrect: <pre>{@code
 * // INCORRECT
 * try (ExternalMapQueryContext<K, V, ?> q = map.queryContext(key)) {
 *     // q.entry(), checks if the entry is present in the map, and acquires
 *     // the read lock for that.
 *     MapEntry<K, V> entry = q.entry();
 *     if (entry != null) {
 *         // Tries to acquire the write lock to perform modification,
 *         // but this is an illegal upgrade: read -> write, throws IllegalMonitorStateException
 *         q.remove(entry);
 *     }
 * }}</pre>
 * So, to workaround this, you should acquire the {@linkplain #updateLock() update lock},
 * which is upgradable to the write lock, <i>before</i> performing any reading in the context:
 * <pre>{@code
 * // CORRECT
 * try (ExternalMapQueryContext<K, V, ?> q = map.queryContext(key)) {
 *     q.updateLock().lock(); // acquire the update lock before checking the entry presence.
 *     MapEntry<K, V> entry = q.entry();
 *     if (entry != null)
 *         q.remove(entry);
 * }}</pre>
 * </li>
 * <li>You want to try to acquire some lock heuristically, in order to improve total {@code
 * ChronicleHash} concurrency. You should base on probabilities of making some reading or
 * writing operations. For example, see how {@link MapMethods#acquireUsing} might be
 * implemented:<pre>{@code
 * default void acquireUsing(MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
 *     // For acquireUsing(), it is assumed to be very probable, that the entry is already
 *     // present in the map, so we will perform the whole acquireUsing() without exclusive locking
 *     if (q.readLock().tryLock()) {
 *         MapEntry<K, V> entry = q.entry();
 *         if (entry != null) {
 *             // Entry is present, return
 *             returnValue.returnValue(entry.value());
 *             return;
 *         }
 *         // Key is absent
 *         // Need to unlock, to lock to update lock later. Direct upgrade is forbidden.
 *         q.readLock().unlock();
 *     }
 *     // We are here, either if we:
 *     // 1) Failed to acquire the read lock, this means some other thread is holding the write
 *     // lock now, in this case waiting for the update lock acquisition is no longer, than for
 *     // the read
 *     // 2) Seen the entry is absent under the read lock. This means we need to insert
 *     // the default value into the map. that requires update-level access as well
 *     q.updateLock().lock();
 *     MapEntry<K, V> entry = q.entry();
 *     if (entry != null) {
 *         // Entry is present, return
 *         returnValue.returnValue(entry.value());
 *         return;
 *     }
 *     // Key is absent
 *     q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
 *     returnValue.returnValue(q.entry().value());
 * }}</pre></li>
 * <li>If the default {@link InterProcessLock#lock()} policy of trying to acquire the
 * lock for some time, and then throw {@code RuntimeException}, or need a custom timeout:
 * <pre>{@code
 * try (ExternalHashQueryContext<K> q = hash.queryContext(key)) {
 *     if (q.writeLock().tryLock(5, TimeUnit.SECONDS)) {
 *         // do something
 *     } else {
 *         // do something else, maybe not throwing an exception
 *     }
 * }}</pre></li>
 * </ul>
 *
 * <p>{@code HashQueryContext} defines the common pattern for working with {@code ChronicleHash}
 * contexts: it has a pair of methods, {@link #entry()} and {@link #absentEntry()}, at any moment
 * one of them returns an (absent) entry context object, another - {@code null}, depending on the
 * presence of the {@linkplain #queriedKey() queried key} in the {@code ChronicleHash}. Thus,
 * block of code that uses {@code HashQueryContext} usually has an if-else statement,
 * with "then" branch for dealing with the present entry, {@code else} branch for dealing with
 * the absent entry, or vise-versa. For example: <pre>{@code
 * interface Point {
 *     double getX();
 *     void setX(double x);
 *     double addX(double xAdd);
 *
 *     double getY();
 *     void setY(double y);
 *     double addY(double yAdd);
 * }
 *
 * <K> Point movePoint(ChronicleMap<K, Point> map, K key, double xMove, double yMove,
 *                     Point using) {
 *     // Moves existing point by [xMove, yMove], if absent - assumes the default point is [0, 0].
 *     // Returns the resulting point
 *     try (ExternalMapQueryContext<K, Point, ?> q = map.queryContext(key)) {
 *         Point offHeapPoint;
 *         q.updateLock().lock();
 *         MapEntry<K, Point> entry = q.entry();
 *         if (entry != null) {
 *             // Key is present
 *             offHeapPoint = entry.value().getUsing(using);
 *         } else {
 *             // Key is absent
 *             q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
 *             offHeapPoint = q.entry().value().getUsing(using);
 *         }
 *         offHeapPoint.addX(xMove);
 *         offHeapPoint.addY(yMove);
 *         return offHeapPoint;
 *     }
 * }}</pre>
 * <p>
 * <p>{@code HashQueryContext} is the base interface defining the structure, but it has no methods
 * to anything "interesting" with {@code entry()} or {@code absentEntry()}. Use {@link
 * MapQueryContext} or {@link SetQueryContext} interfaces, which provide access to {@link
 * MapEntryOperations} and {@link SetEntryOperations} respectively.
 *
 * @param <K> the hash key type
 * @see ChronicleHash#queryContext(Object)
 */
public interface HashQueryContext<K> extends HashContext<K>, SegmentLock {

    /**
     * Returns the index of the accessed segment, where the queried key is located (or to which
     * the key is going to be put).
     * <p>
     * <p>This index might also be used as the {@code InterProcessReadWriteUpdateLock} identifier,
     * because {@code ChronicleHashes} has per-segment locks.
     */
    @Override
    int segmentIndex();

    /**
     * Returns the queried key as a {@code Data}.
     */
    Data<K> queriedKey();

    /**
     * Returns the entry context, if the entry with the queried key is <i>present</i>
     * in the {@code ChronicleHash}, returns {@code null} is the entry is <i>absent</i>.
     *
     * @implNote Might acquire {@link #readLock} before searching for the key, if the context
     * is not locked yet.
     */
    HashEntry<K> entry();

    /**
     * Returns the special <i>absent entry</i> object, if the entry with the queried key
     * is <i>absent</i> in the hash, returns {@code null}, if the entry is <i>present</i>.
     *
     * @implNote Might acquire {@link #readLock} before searching for the key, if the context
     * is not locked yet.
     */
    @Nullable
    HashAbsentEntry<K> absentEntry();
}
