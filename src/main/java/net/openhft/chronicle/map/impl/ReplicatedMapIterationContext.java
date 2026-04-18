/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.iter.IterationAlloc;
import net.openhft.chronicle.hash.impl.stage.iter.IterationKeyHashCode;
import net.openhft.chronicle.hash.impl.stage.iter.IterationSegmentStages;
import net.openhft.chronicle.hash.impl.stage.iter.SegmentsRecovery;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.iter.*;
import net.openhft.chronicle.map.impl.stage.map.*;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,
        ReplicatedChronicleMapHolderImpl.class,

        ReplicatedMapSegmentIteration.class,

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
        ReplicatedMapEntryStages.class,
        ValueBytesInterop.class,
        MapEntryOperationsDelegation.class,

        ReplicationUpdate.class,
        DefaultValue.class,

        IterationAlloc.class,

        ReplicatedTierRecovery.class,
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

        DummyValueZeroData.class,

        ReplicatedMapAbsentDelegatingForIteration.class,
        ReplicatedMapEntryDelegating.class,

        HashEntryChecksumStrategy.class,
}
)
public class ReplicatedMapIterationContext {
}
