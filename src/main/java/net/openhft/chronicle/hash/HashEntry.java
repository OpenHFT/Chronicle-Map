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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapEntryOperations;
import org.jetbrains.annotations.NotNull;

/**
 * Access to the <i>present</i> {@code ChronicleHash} entry.
 * 
 * @param <K> type of the key in {@code ChronicleHash}
 * @see MapEntryOperations
 */
public interface HashEntry<K> {
    /**
     * Returns the context, in which the entry is accessed.
     */
    HashContext<K> context();
    
    /**
     * Returns the entry key.
     */
    @NotNull Value<K, ?> key();

    /**
     * Removes the entry from the {@code ChronicleHash}.
     * 
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     * operation are not met
     */
    void doRemove();
}
