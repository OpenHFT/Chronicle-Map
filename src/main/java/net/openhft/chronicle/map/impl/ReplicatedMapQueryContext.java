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
import net.openhft.chronicle.hash.impl.stage.data.bytes.InputKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.data.instance.InputKeyInstanceData;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.impl.stage.query.QueryHashLookupPos;
import net.openhft.chronicle.hash.impl.stage.query.QueryHashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.data.bytes.*;
import net.openhft.chronicle.map.impl.stage.data.instance.InputValueInstanceData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.input.MapInputBytesValues;
import net.openhft.chronicle.map.impl.stage.input.ReplicatedInput;
import net.openhft.chronicle.map.impl.stage.map.*;
import net.openhft.chronicle.map.impl.stage.query.*;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.impl.stage.ret.BytesReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,
        ThreadLocalCopiesHolder.class,

        LogHolder.class,

        ReplicatedChronicleMapHolderImpl.class,

        KeyBytesInterop.class,
        ReplicatedQuerySegmentStages.class,
        KeySearch.class,
        InputKeyHashCode.class,
        QueryHashLookupSearch.class,
        QueryHashLookupPos.class,

        QueryCheckOnEachPublicOperation.class,
        SearchAllocatedChunks.class,

        ReplicatedMapEntryStages.class,
        MapEntryOperationsDelegation.class,
        WrappedValueInstanceValueHolder.class,
        ReplicatedMapQuery.class,
        ReplicatedMapAbsent.class,
        DefaultValue.class,
        ValueBytesInterop.class,
        ReplicatedMapAbsentHolder.class,
        ReplicationUpdate.class,
        ReplicatedInput.class,

        HashInputBytes.class,
        MapInputBytesValues.class,
},
nested = {
        ReadLock.class,
        UpdateLock.class,
        WriteLock.class,

        EntryKeyBytesData.class,
        EntryValueBytesData.class,

        InputKeyInstanceData.class,
        InputValueInstanceData.class,
        BytesReturnValue.class,

        InputKeyBytesData.class,
        InputFirstValueBytesData.class,
        InputSecondValueBytesData.class,

        WrappedValueInstanceData.class,
        AcquireHandle.class,
        DefaultReturnValue.class,
        UsingReturnValue.class,

        ReplicatedMapAbsentDelegating.class,

        ReplicatedInputKeyBytesData.class,
        ReplicatedInputValueBytesData.class,

        DummyValueZeroData.class,

        HashKeyCrc32PayloadChecksumStrategy.class,
})
public class ReplicatedMapQueryContext {
}
