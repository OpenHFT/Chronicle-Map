/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.stage.entry.ReadLock;
import net.openhft.chronicle.hash.impl.stage.entry.UpdateLock;
import net.openhft.chronicle.hash.impl.stage.entry.WriteLock;
import net.openhft.chronicle.map.impl.stage.query.QueryCheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Ensures public Chronicle Map operations are invoked from the owner thread.
 */
@Staged
public abstract class CheckOnEachPublicOperation {

    @StageRef
    OwnerThreadHolder holder;

    /**
     * Creates a checker for owner-thread enforcement.
     */
    protected CheckOnEachPublicOperation() {
    }

    /**
     * Performs owner-thread check for each public API call.
     */
    public void checkOnEachPublicOperation() {
        checkOnEachLockOperation();
    }

    /**
     * This method call prefix methods in {@link ReadLock}, {@link UpdateLock}, {@link WriteLock}.
     * Distinction from {@link #checkOnEachPublicOperation()} is needed because {@link
     * QueryCheckOnEachPublicOperation#checkOnEachPublicOperation()} depends on some stages that
     * depend on locking methods, closing a dependency cycle.
     */
    public void checkOnEachLockOperation() {
        holder.checkAccessingFromOwnerThread();
    }
}
