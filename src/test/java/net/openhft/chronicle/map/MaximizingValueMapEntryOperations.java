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

public class MaximizingValueMapEntryOperations<K, V extends Comparable<? super V>>
        implements MapEntryOperations<K, V> {

    @Override
    public boolean replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
        MapContext<K, V> context = entry.context();
        if (context instanceof ReplicationContext) {
            int compareResult = newValue.get().compareTo(entry.value().get());
            if (compareResult > 0 || (compareResult == 0 &&
                    ((ReplicationContext) context).remoteIdentifier() <=
                            ((ReplicatedEntry) entry).originIdentifier())) {
                // replace, if the new value is greater
                entry.doReplaceValue(newValue);
                return true;

            } else {
                // always fail, if the value came via replication is lesser than the current value.
                return false;
            }
        } else {
            return MapEntryOperations.super.replaceValue(entry, newValue);
        }
    }
}
