//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashSegmentContext;

/**
 * Context of {@link ChronicleSet}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleSet}
 * @param <R> the return type of {@link SetEntryOperations} specified for the queried set
 * @see ChronicleSet#segmentContext(int)
 */
public interface SetSegmentContext<K, R>
        extends HashSegmentContext<K, SetEntry<K>>, SetContext<K, R> {
}
