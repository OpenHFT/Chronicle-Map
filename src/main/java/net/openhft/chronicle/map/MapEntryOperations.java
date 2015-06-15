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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;

/**
 * SPI interface for customizing "low-level" modification operations on {@link ChronicleMap}
 * entries. All {@code ChronicleMap} modifications operate via and instance of this interface:
 * <ul>
 *     <li>Ordinary query operations: {@code map.put()}, {@code map.compute()}. In fact, the</li>
 * </ul>
 *
 * @param <K> the map key type
 * @param <V> the map value type
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
     */
    default R remove(@NotNull MapEntry<K, V> entry) {
        entry.doRemove();
        return null;
    }
    
    /**
     * Replaces the given entry's value with the new one.
     *
     * @param entry the entry to replace the value in
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     * operation are not met
     */
    default R replaceValue(@NotNull MapEntry<K, V> entry, Data<V, ?> newValue) {
        entry.doReplaceValue(newValue);
        return null;
    }

    /**
     * Inserts the new entry into the map, of {@link MapAbsentEntry#absentKey() the key} from
     * the given insertion context (<code>absentEntry</code>) and the given {@code value}.
     * Returns {@code true} if the insertion  was successful, {@code false} if it was discarded
     * for any reason.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     * operation are not met
     */
    default R insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        absentEntry.doInsert(value);
        return null;
    }

    /**
     * Returns the "nil" value, which should be inserted into the map, in the given
     * {@code absentEntry} context. This is primarily used in {@link ChronicleMap#acquireUsing}
     * operation implementation, i. e. {@link MapMethods#acquireUsing}.
     *
     * @implNote simply delegates to {@link MapAbsentEntry#defaultValue()}.
     */
    default Data<V, ?> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
        return absentEntry.defaultValue();
    }
}
