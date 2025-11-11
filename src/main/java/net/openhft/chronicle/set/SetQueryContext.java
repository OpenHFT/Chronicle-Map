/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * A context of {@link ChronicleSet} operations with <i>individual keys</i>
 * (most: {@code contains()}, {@code add()}, etc., opposed to <i>bulk</i> operations).
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see ChronicleSet#queryContext(Object)
 */
public interface SetQueryContext<K, R> extends HashQueryContext<K>, SetContext<K, R> {

    @Override
    @Nullable
    SetEntry<K> entry();

    @Override
    @Nullable
    SetAbsentEntry<K> absentEntry();
}
