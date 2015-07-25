/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.NotNull;

/**
 * SPI interface for customizing "low-level" modification operations on {@link ChronicleMap}
 * entries. All {@code ChronicleMap} modifications operate via an instance of this interface:
 * <ul>
 *     <li>Ordinary query operations: {@code map.put()}, {@code map.compute()}, {@code map.remove()}
 *     </li>
 *     <li>Operations with entry during iterations on the map, or the key/value/entry views,
 *     like {@code iterator.remove()}.</li>
 *     <li>Remote map operation calls and Replicated {@code ChronicleMap} reconciliation operations:
 *     see {@link MapRemoteOperations}.</li>
 * </ul>
 *
 * <p>By default {@link #remove}, {@link #insert} and {@link #replaceValue} return {@code null},
 * but subclasses could return something more sensible, to be used in higher-level SPI interfaces,
 * namely {@link MapMethods} and {@link MapRemoteOperations}. For example, in bidirectional map
 * implementation (i. e. a map that preserves the uniqueness of its values as well as that of
 * its keys), that includes two {@code ChronicleMaps}, the {@code MapEntryOperations}' methods return
 * type could be used to indicate if we were successful to lock both maps before performing
 * the update: <pre>{@code
 * enum DualLockSuccess {SUCCESS, FAIL}
 *
 * class BiMapEntryOperations<K, V> implements MapEntryOperations<K, V, DualLockSuccess> {
 *     ChronicleMap<V, K> reverse;
 *
 *     public void setReverse(ChronicleMap<V, K> reverse) {
 *         this.reverse = reverse;
 *     }
 *
 *     @Override
 *     public DualLockSuccess remove(@NotNull MapEntry<K, V> entry) {
 *         try (ExternalMapQueryContext<V, K, ?> rq = reverse.queryContext(entry.value())) {
 *             if (!rq.updateLock().tryLock()) {
 *                 if (entry.context() instanceof MapQueryContext)
 *                     return FAIL;
 *                 throw new IllegalStateException("Concurrent modifications to reverse map " +
 *                         "during remove during iteration");
 *             }
 *             MapEntry<V, K> reverseEntry = rq.entry();
 *             if (reverseEntry != null) {
 *                 entry.doRemove();
 *                 reverseEntry.doRemove();
 *                 return SUCCESS;
 *             } else {
 *                 throw new IllegalStateException(entry.key() + " maps to " + entry.value() +
 *                         ", but in the reverse map this value is absent");
 *             }
 *         }
 *     }
 *
 *     // ... other methods
 * }
 *
 * class BiMapMethods<K, V> implements MapMethods<K, V, DualLockSuccess> {
 *     @Override
 *     public void remove(MapQueryContext<K, V, DualLockSuccess> q, ReturnValue<V> returnValue) {
 *         while (true) {
 *             q.updateLock().lock();
 *             try {
 *                 MapEntry<K, V> entry = q.entry();
 *                 if (entry != null) {
 *                     returnValue.returnValue(entry.value());
 *                     if (q.remove(entry) == SUCCESS)
 *                         return;
 *                 }
 *             } finally {
 *                 q.readLock().unlock();
 *             }
 *         }
 *     }
 *
 *     // ... other methods
 * }}</pre>
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> methods return type, used for communication between lower- and higher-level SPI
 * @see ChronicleMapBuilder#entryOperations(MapEntryOperations)           
 */
public interface MapEntryOperations<K, V, R> {
    
    /**
     * Removes the given entry from the map.
     * 
     * @implNote default implementation calls {@link MapEntry#doRemove()} on the given entry
     * and returns {@code null}.
     *
     * @param entry the entry to remove 
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     * operation are not met
     * @throws RuntimeException if removal was unconditionally unsuccessful due to any reason
     */
    default R remove(@NotNull MapEntry<K, V> entry) {
        entry.doRemove();
        return null;
    }
    
    /**
     * Replaces the given entry's value with the new one.
     *
     * @implNote default implementation calls {@link MapEntry#doReplaceValue(Data)
     * entry.doReplaceValue(newValue)} and returns {@code null}.
     *
     * @param entry the entry to replace the value in
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     * operation are not met
     * @throws RuntimeException if value replacement was unconditionally unsuccessful due
     * to any reason
     */
    default R replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        entry.doReplaceValue(newValue);
        return null;
    }

    /**
     * Inserts the new entry into the map, of {@link MapAbsentEntry#absentKey() the key} from
     * the given insertion context (<code>absentEntry</code>) and the given {@code value}.
     *
     * @implNote default implementation calls {@link MapAbsentEntry#doInsert(Data)
     * absentEntry.doInsert(value)} and returns {@code null}.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     * operation are not met
     * @throws RuntimeException if insertion was unconditionally unsuccessful due to any reason
     */
    default R insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        absentEntry.doInsert(value);
        return null;
    }
}
