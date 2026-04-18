/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class QueryHashLookupSearch extends HashLookupSearch {

    @StageRef
    KeyHashCode h;

    void initSearchKey() {
        initSearchKey(hl().maskUnsetKey(hh.h().hashSplitting.segmentHash(h.keyHashCode())));
    }
}
