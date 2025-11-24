/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * {@link CheckOnEachPublicOperation} specialisation for map queries.
 * <p>
 * In addition to checking that the context is still open, this stage asks the
 * underlying {@link HashQuery} to drop the current search if nested-context
 * validation fails, preventing callers from continuing to use a stale hash
 * lookup slot.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueryCheckOnEachPublicOperation extends CheckOnEachPublicOperation {

    @StageRef
    HashQuery q;

    @Override
    public void checkOnEachPublicOperation() {
        super.checkOnEachPublicOperation();
        q.dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed();
    }
}
