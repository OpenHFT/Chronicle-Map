/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
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

        LogHolder.class,

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
}, nested = {
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
})
public class ReplicatedMapIterationContext {
}
