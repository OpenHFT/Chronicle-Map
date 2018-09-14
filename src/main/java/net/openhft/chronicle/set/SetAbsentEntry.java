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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new key is going to be inserted
 * into the {@link ChronicleSet}.
 *
 * @param <K> the set key type
 * @see SetEntryOperations
 * @see SetQueryContext#absentEntry()
 */
public interface SetAbsentEntry<K> extends HashAbsentEntry<K> {
    @NotNull
    @Override
    SetContext<K, ?> context();

    /**
     * Inserts {@link #absentKey() the new key} into the set.
     * <p>
     * <p>This method is the default implementation for {@link SetEntryOperations#insert(
     *SetAbsentEntry)}, which might be customized over the default.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @see SetEntryOperations#insert(SetAbsentEntry)
     */
    void doInsert();
}
