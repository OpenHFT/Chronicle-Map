/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;

/**
 * {@link InterProcessReadWriteUpdateLock} of a segment in {@code ChronicleHash}.
 * <p>
 * In Chronicle-Map off-heap design, locks (and concurrency) are per-segment.
 *
 * @see ChronicleHashBuilder#minSegments(int)
 * @see ChronicleHashBuilder#actualSegments(int)
 */
public interface SegmentLock extends InterProcessReadWriteUpdateLock {

    /**
     * Returns the index of the accessed segment.
     */
    int segmentIndex();
}
