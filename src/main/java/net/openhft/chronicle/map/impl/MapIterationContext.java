/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.iter.*;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.iter.IterationCheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.iter.MapSegmentIteration;
import net.openhft.chronicle.map.impl.stage.map.*;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,
        VanillaChronicleMapHolderImpl.class,

        MapSegmentIteration.class,

        KeyBytesInterop.class,
        IterationSegmentStages.class,
        IterationKeyHashCode.class,
        HashLookupPos.class,
        IterationCheckOnEachPublicOperation.class,
        AllocatedChunks.class,
        KeySearch.class,
        HashLookupSearch.class,

        WrappedValueInstanceDataHolderAccess.class,
        WrappedValueBytesDataAccess.class,
        MapEntryStages.class,
        ValueBytesInterop.class,
        MapEntryOperationsDelegation.class,

        IterationAlloc.class,

        TierRecovery.class,
        SegmentsRecovery.class,
}
, nested = {
        ReadLock.class,
        UpdateLock.class,
        WriteLock.class,

        EntryKeyBytesData.class,
        EntryValueBytesData.class,

        WrappedValueInstanceDataHolder.class,
        WrappedValueBytesData.class,

        HashEntryChecksumStrategy.class,
}
)
public class MapIterationContext {
}
