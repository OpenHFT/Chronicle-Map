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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.core.io.Closeable;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Context of {@link ChronicleHash}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleHash}
 * @param <E> the entry type
 * @see ChronicleHash#segmentContext(int)
 */
public interface HashSegmentContext<K, E extends HashEntry<K>> extends HashContext<K>, Closeable {

    /**
     * Performs the given action for each <i>present</i> entry in this segment until all entries
     * have been processed or the action throws an {@code Exception}. Exceptions thrown by the
     * action are relayed to the caller.
     *
     * @param action the action to be performed for each entry in this segment
     */
    void forEachSegmentEntry(Consumer<? super E> action);

    /**
     * Checks the given predicate on each <i>present</i> entry in this segment until all entries
     * have been processed or the predicate returns {@code false} for some entry, or throws
     * an {@code Exception}. Exceptions thrown by the predicate are relayed to the caller.
     * <p>
     * <p>If this segment is empty (i. e. {@link #size()} call returns 0), this method returns
     * {@code true} immediately.
     *
     * @param predicate the predicate to be checked for each entry in this segment
     * @return {@code true} if the predicate returned {@code true} for all checked entries,
     * {@code false} if it returned {@code false} for some entry
     */
    boolean forEachSegmentEntryWhile(Predicate<? super E> predicate);

    /**
     * Returns the number of <i>present</i> entries in this segment.
     */
    long size();
}
