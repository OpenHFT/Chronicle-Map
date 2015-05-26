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

package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.AcceptanceDecision;
import net.openhft.chronicle.hash.Value;
import net.openhft.chronicle.hash.replication.HashReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;

import static net.openhft.chronicle.hash.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

public interface MapRemoteOperations<K, V, R> {
    
    default AcceptanceDecision remove(MapRemoteQueryContext<K, V, R> q) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (absentEntry instanceof HashReplicableEntry) {
                return decideOnRemoteModification((HashReplicableEntry<?>) absentEntry, q);
            } else {
                return ACCEPT;
            }
        }
    }
    
    default AcceptanceDecision put(MapRemoteQueryContext<K, V, R> q, Value<V, ?> newValue) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.replaceValue(entry, newValue);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (!(absentEntry instanceof HashReplicableEntry) ||
                    decideOnRemoteModification((HashReplicableEntry<?>) absentEntry, q) == ACCEPT) {
                q.insert(absentEntry, newValue);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        }
    }
}
