/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

/**
 * Marker interface for iteration contexts operating over replicated
 * Chronicle-Map segments.
 * <p>
 * In addition to the standard {@link IterationContext} responsibilities,
 * implementations can materialise an existing entry at a particular position
 * via {@link #readExistingEntry(long)} so that replication metadata and
 * deletion flags can be inspected or repaired during recovery and iteration.
 * <p>
 * This interface is used only by internal staged code and is not intended for
 * direct use by applications.
 */
public interface ReplicatedIterationContext<K, V, R> extends IterationContext<K, V, R> {

    void readExistingEntry(long pos);
}
