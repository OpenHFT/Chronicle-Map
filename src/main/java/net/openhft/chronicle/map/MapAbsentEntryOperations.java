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
 * SPI interface for customizing "low-level" insertion of a new entry into {@link ChronicleMap}.
 *
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 */
public interface MapAbsentEntryOperations<K, V> {
    
    static boolean shoudInsert(@NotNull MapAbsentEntry<?, ?> absentEntry) {
        MapContext<?, ?> context = absentEntry.context();
        return !(context instanceof ReplicationContext) ||
                !(absentEntry instanceof ReplicatedEntry) ||
                shouldApplyRemoteModification((ReplicatedEntry) absentEntry,
                        (ReplicationContext) context);
    }

    /**
     * Inserts the new entry into the map, of {@link MapAbsentEntry#absentKey() the key} from
     * the given insertion context (<code>absentEntry</code>) and the given {@code value}.
     * Returns {@code true} if the insertion  was successful, {@code false} if it failed
     * for any reason.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     * operation are not met
     */
    default boolean insert(@NotNull MapAbsentEntry<K, V> absentEntry, Value<V, ?> value) {
        if (shoudInsert(absentEntry)) {
            absentEntry.doInsert(value);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the "nil" value, which should be inserted into the map, in the given
     * {@code absentEntry} context. This is primarily used in {@link ChronicleMap#acquireUsing}
     * operation implementation, i. e. {@link MapMethods#acquireUsing}.
     * 
     * @implNote simply delegates to {@link MapAbsentEntry#defaultValue()}.
     */
    default Value<V, ?> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
        return absentEntry.defaultValue();
    }
}
