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

import net.openhft.chronicle.hash.HashContext;
import net.openhft.chronicle.hash.Value;

/**
 * Context, in which {@link MapEntry MapEntries} are accessed.
 * 
 * @param <K> the key type of accessed {@link ChronicleMap}
 * @param <V> the value type of accessed {@code ChronicleMap}
 */
public interface MapContext<K, V, R> extends HashContext<K>, MapEntryOperations<K, V, R> {
    /**
     * Returns the accessed {@code ChronicleMap}.
     */
    @Override
    default ChronicleMap<K, V> hash() {
        return map();
    }

    /**
     * Synonym to {@link #hash()}.
     */
    ChronicleMap<K, V> map();
    
    Value<V, ?> wrapValueAsValue(V value);
}
