/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
     * Note: default implementation calls {@link SetEntry#doRemove()} on the given entry and
     * returns {@code null}.
     *
     * @param entry the entry to remove
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     * @throws RuntimeException      if removal was unconditionally unsuccessful due to any reason
     */
    default R remove(@NotNull SetEntry<K> entry) {
        entry.doRemove();
        return null;
    }

    /**
     * Inserts the new entry into the set, of {@link SetAbsentEntry#absentKey() the key} from
     * the given insertion context ({@code absentEntry}).
     * Note: default implementation calls {@link SetAbsentEntry#doInsert()} and returns
     * {@code null}.
     *
     * @return result of operation, understandable by higher-level SPIs
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @throws RuntimeException      if insertion was unconditionally unsuccessful due to any reason
     */
    default R insert(@NotNull SetAbsentEntry<K> absentEntry) {
        absentEntry.doInsert();
        return null;
    }
}
