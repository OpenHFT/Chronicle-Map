/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.data.bytes.InputKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.data.instance.InputKeyInstanceData;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookup;
import net.openhft.chronicle.hash.impl.stage.entry.ReadLock;
import net.openhft.chronicle.hash.impl.stage.entry.UpdateLock;
import net.openhft.chronicle.hash.impl.stage.entry.WriteLock;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.chronicle.hash.impl.stage.query.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.query.QueryHashLookupPos;
import net.openhft.chronicle.hash.impl.stage.query.QuerySegmentStages;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.data.bytes.*;
import net.openhft.chronicle.map.impl.stage.data.instance.InputValueInstanceData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
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

        HashLookup.class,
        KeyBytesInterop.class,
        QuerySegmentStages.class,

        HashLookupSearch.class,
        QueryHashLookupPos.class,

        QueryCheckOnEachPublicOperation.class,
        SearchAllocatedChunks.class,

        ReplicatedMapEntryStages.class,
        MapEntryOperationsDelegation.class,
        WrappedValueInstanceValueHolder.class,
        ReplicatedMapQuery.class,
        ReplicatedMapAbsent.class,
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
        DeprecatedMapKeyContextOnQuery.class,
        DeprecatedMapAcquireContextOnQuery.class,
        DefaultReturnValue.class,
        UsingReturnValue.class,

        ReplicatedMapAbsentDelegating.class,

        ReplicatedInputKeyBytesData.class,
        ReplicatedInputValueBytesData.class,

        DummyValueZeroData.class,
})
public class ReplicatedMapQueryContext {
}
