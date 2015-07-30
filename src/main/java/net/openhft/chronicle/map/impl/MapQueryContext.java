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
import net.openhft.chronicle.hash.impl.stage.entry.HashLookup;
import net.openhft.chronicle.hash.impl.stage.entry.ReadLock;
import net.openhft.chronicle.hash.impl.stage.entry.UpdateLock;
import net.openhft.chronicle.hash.impl.stage.entry.WriteLock;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.chronicle.hash.impl.stage.query.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.query.QueryHashLookupPos;
import net.openhft.chronicle.hash.impl.stage.query.QuerySegmentStages;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputFirstValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputSecondValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.InputValueInstanceData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.chronicle.map.impl.stage.input.MapInputBytesValues;
import net.openhft.chronicle.map.impl.stage.map.MapEntryOperationsDelegation;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.chronicle.map.impl.stage.map.VanillaChronicleMapHolderImpl;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceValueHolder;
import net.openhft.chronicle.map.impl.stage.query.*;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,
        ThreadLocalCopiesHolder.class,

        VanillaChronicleMapHolderImpl.class,

        HashLookup.class,
        KeyBytesInterop.class,
        QuerySegmentStages.class,

        HashLookupSearch.class,
        QueryHashLookupPos.class,

        QueryCheckOnEachPublicOperation.class,
        SearchAllocatedChunks.class,

        QueryMapEntryStages.class,
        MapEntryOperationsDelegation.class,
        WrappedValueInstanceValueHolder.class,
        MapQuery.class,
        MapAbsent.class,
        ValueBytesInterop.class,
        MapAbsentHolder.class,

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

        InputKeyBytesData.class,
        InputFirstValueBytesData.class,
        InputSecondValueBytesData.class,

        WrappedValueInstanceData.class,
        AcquireHandle.class,
        DefaultReturnValue.class,
        UsingReturnValue.class,
})
public class MapQueryContext {
}
