/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * {@link CheckOnEachPublicOperation} specialisation used during map
 * iteration.
 * <p>
 * In addition to the standard closed-state checks performed by the base
 * class, this implementation verifies on every public operation that the
 * current entry has not been removed during the iteration, failing fast if
 * the contract is violated.
 */
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
