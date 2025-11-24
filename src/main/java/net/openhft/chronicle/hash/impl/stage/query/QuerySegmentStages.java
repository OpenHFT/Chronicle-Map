/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Specialised {@link SegmentStages} implementation for query contexts.
 *
 * <p>In addition to the base segment bookkeeping, this stage computes the segment index directly
 * from the current key hash code, ensuring that all subsequent staged operations work against the
 * correct segment and tier chain for the queried key.
 */
@Staged
public abstract class QuerySegmentStages extends SegmentStages {

    @StageRef
    KeyHashCode h;

    void initSegmentIndex() {
        segmentIndex = hh.h().hashSplitting.segmentIndex(h.keyHashCode());
    }
}
