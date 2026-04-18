/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

/**
 * Root interface for contexts, in which {@link HashEntry HashEntries} could be accessed.
 *
 * @param <K> the key type of accessed {@link ChronicleHash}
 */
public interface HashContext<K> {
    /**
     * Returns the accessed {@code ChronicleHash}.
     */
    ChronicleHash<K, ?, ?, ?> hash();
}
