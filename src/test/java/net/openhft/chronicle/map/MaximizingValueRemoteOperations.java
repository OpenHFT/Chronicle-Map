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

import net.openhft.chronicle.hash.AcceptanceDecision;
import net.openhft.chronicle.hash.Value;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.map.replication.MapReplicableEntry;

import static net.openhft.chronicle.hash.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.AcceptanceDecision.DISCARD;

// TODO verify eventual consistency
public class MaximizingValueRemoteOperations<K, V extends Comparable<? super V>, R>
        implements MapRemoteOperations<K, V, R> {
    
    @Override
    public AcceptanceDecision put(MapRemoteQueryContext<K, V, R> q, Value<V, ?> newValue) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            int compareResult = newValue.get().compareTo(entry.value().get());
            if (compareResult > 0 ||
                    (compareResult == 0 && q.remoteIdentifier() <= entry.originIdentifier())) {
                // replace, if the new value is greater
                entry.doReplaceValue(newValue);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        } else {
            return MapRemoteOperations.super.put(q, newValue);
        }
    }
}
