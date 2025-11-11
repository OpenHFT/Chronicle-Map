/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class QuerySegmentStages extends SegmentStages {

    @StageRef
    KeyHashCode h;

    void initSegmentIndex() {
        segmentIndex = hh.h().hashSplitting.segmentIndex(h.keyHashCode());
    }
}
