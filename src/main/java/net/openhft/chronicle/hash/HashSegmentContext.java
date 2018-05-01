/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
