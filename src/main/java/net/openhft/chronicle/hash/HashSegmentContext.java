/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapClosable;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Context of {@link ChronicleHash}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleHash}
 * @param <E> the entry type
 * @see ChronicleHash#segmentContext(int)
 */
public interface HashSegmentContext<K, E extends HashEntry<K>> extends HashContext<K>, MapClosable {

    /**
     * Performs the given action for each <i>present</i> entry in this segment until all entries have been processed or the action throws an {@code
     * Exception}. Exceptions thrown by the action are relayed to the caller.
     *
     * @param action the action to be performed for each entry in this segment
     */
    void forEachSegmentEntry(Consumer<? super E> action);

    /**
     * Checks the given predicate on each <i>present</i> entry in this segment until all entries have been processed or the predicate returns {@code
     * false} for some entry, or throws an {@code Exception}. Exceptions thrown by the predicate are relayed to the caller.
     * <p>
     * If this segment is empty (i. e. {@link #size()} call returns 0), this method returns
     * {@code true} immediately.
     *
     * @param predicate the predicate to be checked for each entry in this segment
     * @return {@code true} if the predicate returned {@code true} for all checked entries, {@code false} if it returned {@code false} for some entry
     */
    boolean forEachSegmentEntryWhile(Predicate<? super E> predicate);

    /**
     * Returns the number of <i>present</i> entries in this segment.
     */
    long size();
}
