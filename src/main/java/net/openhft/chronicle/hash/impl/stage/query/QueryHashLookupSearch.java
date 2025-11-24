/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Query-time specialisation of {@link HashLookupSearch} that derives the search key from
 * the current {@link KeyHashCode}.
 *
 * <p>The stage initialises the hash-lookup search key using the segmented hash of the
 * current key so that subsequent probing operations only need to step through slots.
 */
@Staged
public abstract class QueryHashLookupSearch extends HashLookupSearch {

    @StageRef
    KeyHashCode h;

    void initSearchKey() {
        initSearchKey(hl().maskUnsetKey(hh.h().hashSplitting.segmentHash(h.keyHashCode())));
    }
}
