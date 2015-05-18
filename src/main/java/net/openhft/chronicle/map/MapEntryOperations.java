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

import net.openhft.chronicle.hash.Value;
import net.openhft.chronicle.hash.replication.ReplicatedEntry;
import net.openhft.chronicle.hash.replication.ReplicationContext;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.shouldApplyRemoteModification;

/**
 * SPI interface for customizing "low-level" modification operations on the <i>present</i>
 * {@link ChronicleMap} entry.
 *
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 */
public interface MapEntryOperations<K, V> {
    
    static boolean shouldModify(@NotNull MapEntry<?, ?> entry) {
        MapContext<?, ?> context = entry.context();
        return !(context instanceof ReplicationContext) ||
                shouldApplyRemoteModification((ReplicatedEntry) entry,
                        (ReplicationContext) context);
    }
    
    /**
     * Removes the given entry from the {@code ChronicleHash}. Returns {@code true} if the remove
     * was successful, {@code false} if it failed for any reason.
     *
     * @param entry the entry to remove 
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     * operation are not met
     */
    default boolean remove(@NotNull MapEntry<K, V> entry) {
        if (shouldModify(entry)) {
            entry.doRemove();
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * Replaces the given entry's value with the new one. Returns {@code true} if the replace
     * operation was successful, {@code false} if it failed for any reason.
     *
     * @param entry the entry to replace the value in 
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     * operation are not met
     */
    default boolean replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
        if (shouldModify(entry)) {
            entry.doReplaceValue(newValue);
            return true;
        } else {
            return false;
        }
    }
}
