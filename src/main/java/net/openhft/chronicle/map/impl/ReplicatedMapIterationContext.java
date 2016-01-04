/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
