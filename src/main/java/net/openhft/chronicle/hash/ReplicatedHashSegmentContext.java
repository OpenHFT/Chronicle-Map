/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Segment-level context for replicated ChronicleHash operations.
 * <p>
 * Extends {@link HashSegmentContext} with iteration hooks over {@link ReplicableEntry} instances so
 * replication code can visit only the entries within the currently locked segment.
 */
public interface ReplicatedHashSegmentContext<K, E extends HashEntry<K>>
        extends HashSegmentContext<K, E> {

    void forEachSegmentReplicableEntry(Consumer<? super ReplicableEntry> action);

    boolean forEachSegmentReplicableEntryWhile(Predicate<? super ReplicableEntry> predicate);
}
