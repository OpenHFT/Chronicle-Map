/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.SetEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@code ChronicleHash}.
 * <p>
 * This interface is not usable by itself; it merely defines the common base for {@link MapEntry}
 * and {@link SetEntry}.
 *
 * @param <K> type of the key in {@code ChronicleHash}
 * @see HashQueryContext#entry()
 */
public interface HashEntry<K> {
    /**
     * Returns the context, in which the entry is accessed.
     *
     * @return owning hash context
     */
    HashContext<K> context();

    /**
     * Returns the entry key.
     *
     * @return key data
     */
    @NotNull
    Data<K> key();

    /**
     * Removes the entry from the {@code ChronicleHash}.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     */
    void doRemove();
}
