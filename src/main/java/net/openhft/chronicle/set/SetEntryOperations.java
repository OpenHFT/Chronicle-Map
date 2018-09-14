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

import org.jetbrains.annotations.NotNull;

/**
 * SPI interface fro customizing "low-level" modification operations on {@link ChronicleSet}
 * entries.
 *
 * @param <K> the set key type
 * @param <R> methods return type, used for communication between lower- and higher-level SPI
 * @see ChronicleSetBuilder#entryOperations(SetEntryOperations)
 */
public interface SetEntryOperations<K, R> {

    /**
     * Removes the given entry from the set.
     *
     * @param entry the entry to remove
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     * @throws RuntimeException      if removal was unconditionally unsuccessful due to any reason
     * @implNote default implementation calls {@link SetEntry#doRemove()} on the given entry and
     * returns {@code null}.
     */
    default R remove(@NotNull SetEntry<K> entry) {
        entry.doRemove();
        return null;
    }

    /**
     * Inserts the new entry into the set, of {@link SetAbsentEntry#absentKey() the key} from
     * the given insertion context (<code>absentEntry</code>).
     *
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @throws RuntimeException      if insertion was unconditionally unsuccessful due to any reason
     * @implNote default implementation calls {@link SetAbsentEntry#doInsert()} and returns
     * {@code null}.
     */
    default R insert(@NotNull SetAbsentEntry<K> absentEntry) {
        absentEntry.doInsert();
        return null;
    }
}
