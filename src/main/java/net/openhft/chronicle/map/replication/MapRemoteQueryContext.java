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

import net.openhft.chronicle.hash.replication.HashRemoteQueryContext;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntryOperations;
import net.openhft.chronicle.map.MapQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleMap} queries and internal replication operations.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specialized for the queried map
 * @see MapRemoteOperations
 */
public interface MapRemoteQueryContext<K, V, R> extends MapQueryContext<K, V, R>,
        HashRemoteQueryContext<K> {
    @Nullable
    @Override
    MapReplicableEntry<K, V> entry();
}
