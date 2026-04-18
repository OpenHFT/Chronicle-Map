/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

import java.util.function.Consumer;
import java.util.function.Predicate;

public interface ReplicatedHashSegmentContext<K, E extends HashEntry<K>>
        extends HashSegmentContext<K, E> {

    void forEachSegmentReplicableEntry(Consumer<? super ReplicableEntry> action);

    boolean forEachSegmentReplicableEntryWhile(Predicate<? super ReplicableEntry> predicate);
}
