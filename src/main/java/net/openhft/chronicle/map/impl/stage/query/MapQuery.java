/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.MapAbsentEntryHolder;
import net.openhft.chronicle.map.impl.QueryContextInterface;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputFirstValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputSecondValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.InputValueInstanceData;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.impl.stage.query.HashQuery.SearchState.PRESENT;

@Staged
public abstract class MapQuery<K, V, R> extends HashQuery<K>
        implements MapEntry<K, V>, ExternalMapQueryContext<K, V, R>,
        QueryContextInterface<K, V, R> {

    @StageRef VanillaChronicleMapHolder<K, ?, ?, V, ?, ?, R> mh;
    @StageRef MapEntryStages<K, V> e;
    @StageRef SearchAllocatedChunks allocatedChunks;
    @StageRef public DeprecatedMapKeyContextOnQuery<K, V> deprecatedMapKeyContext;
    @StageRef public AcquireHandle<K, V> acquireHandle;
    
    @StageRef public InputValueInstanceData<V, ?, ?> inputValueInstanceValue;
    
    @StageRef public InputFirstValueBytesData<V> inputFirstValueBytesValue;
    @StageRef public InputSecondValueBytesData<V> inputSecondValueBytesValue;
    
    @StageRef public DefaultReturnValue<V> defaultReturnValue;
    @StageRef public UsingReturnValue<V> usingReturnValue;

    @StageRef public MapAbsentEntryHolder<K, V> absent;

    @Override
    public MapEntry<K, V> entry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? null : absent.absent();
    }
    
    protected void putPrefix() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        boolean underUpdatedLockIsHeld = !s.innerUpdateLock.isHeldByCurrentThread();
        if (underUpdatedLockIsHeld)
            s.innerUpdateLock.lock();
        boolean searchResultsNotTrusted = underUpdatedLockIsHeld ||
                s.concurrentSameThreadContexts;
        if (hlp.hashLookupPosInit() && searchStateAbsent() && searchResultsNotTrusted)
            hlp.closeHashLookupPos();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        putPrefix();
        if (searchStatePresent()) {
            e.innerDefaultReplaceValue(newValue);
            s.incrementModCount();
            setSearchState(PRESENT);
        } else {
            throw new IllegalStateException(
                    "Entry is absent in the map when doReplaceValue() is called");
        }
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return this;
    }
}
