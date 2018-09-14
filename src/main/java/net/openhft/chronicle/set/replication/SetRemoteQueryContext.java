/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetEntryOperations;
import net.openhft.chronicle.set.SetQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleSet} queries and internal replication operations.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see SetRemoteOperations
 */
public interface SetRemoteQueryContext<K, R>
        extends SetQueryContext<K, R>, RemoteOperationContext<K> {
    @Override
    @Nullable
    SetReplicableEntry<K> entry();
}
