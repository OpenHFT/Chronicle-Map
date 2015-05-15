

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

import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.Value;

/**
 * Access to the <i>present</i> {@link ChronicleMap} entry.
 * 
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 *            
 * @see MapEntryOperations
 */
public interface MapEntry<K, V> extends HashEntry<K> {
    /**
     * Returns the entry value.
     */
    Value<V, ?> value();

    /**
     * Replaces the entry's value with the new one. Returns {@code true} if the replace operation
     * was successful, {@code false} if it failed for any reason.
     *
     * <p>This method if the default implementation for {@link MapEntryOperations#replaceValue},
     * which might be customized over the default.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     * operation are not met
     */
    boolean defaultReplaceValue(Value<V, ?> newValue);
}

