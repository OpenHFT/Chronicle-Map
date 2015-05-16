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
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import org.jetbrains.annotations.Nullable;

/**
 * The context of Map operations with <i>individual keys</i> (most: get(), put(), etc., opposed
 * to bulk operations).
 *  
 * @param <K>
 * @param <V>
 */
public interface MapQueryContext<K, V> extends InterProcessReadWriteUpdateLock,
        MapEntryOperations<K, V>, MapAbsentEntryOperations<K, V> {

    /**
     * Returns the queried key as a {@code Value}.
     */
    Value<K, ?> queriedKey();

    /**
     * Returns the entry access object, if the entry with the queried key is <i>present</i>
     * in the map, returns {@code null} is the entry is <i>absent</i>. Might acquire
     * {@link #readLock} before searching for the key, if the context is not locked yet.
     */
    @Nullable
    MapEntry<K, V> entry();

    /**
     * Returns the special <i>absent entry</i> object, if the entry with the queried key
     * is <i>absent</i> in the map, returns {@code null}, if the entry is <i>present</i>. Might
     * acquire {@link #readLock} before searching for the key, if the context is not locked yet.
     */
    @Nullable
    MapAbsentEntry<K, V> absentEntry();
}
