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
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.iter.DeprecatedMapKeyContextOnIteration;
import net.openhft.chronicle.map.impl.stage.iter.IterationCheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.iter.MapSegmentIteration;
import net.openhft.chronicle.map.impl.stage.map.MapEntryOperationsDelegation;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.chronicle.map.impl.stage.map.VanillaChronicleMapHolderImpl;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceValueHolder;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,
        ThreadLocalCopiesHolder.class,
        VanillaChronicleMapHolderImpl.class,

        MapSegmentIteration.class,

        HashLookup.class,
        KeyBytesInterop.class,
        SegmentStages.class,
        HashLookupPos.class,
        IterationCheckOnEachPublicOperation.class,
        AllocatedChunks.class,

        WrappedValueInstanceValueHolder.class,
        MapEntryStages.class,
        ValueBytesInterop.class,
        MapEntryOperationsDelegation.class,
}, nested = {
        ReadLock.class,
        UpdateLock.class,
        WriteLock.class,

        EntryKeyBytesData.class,
        EntryValueBytesData.class,

        WrappedValueInstanceData.class,

        DeprecatedMapKeyContextOnIteration.class,
})
public class MapIterationContext {
}
