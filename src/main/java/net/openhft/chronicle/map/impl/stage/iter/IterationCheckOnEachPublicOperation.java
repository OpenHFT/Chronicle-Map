//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class IterationCheckOnEachPublicOperation extends CheckOnEachPublicOperation {

    @StageRef
    MapSegmentIteration<?, ?, ?> iteration;

    @Override
    public void checkOnEachPublicOperation() {
        super.checkOnEachPublicOperation();
        iteration.checkEntryNotRemovedOnThisIteration();
    }
}
