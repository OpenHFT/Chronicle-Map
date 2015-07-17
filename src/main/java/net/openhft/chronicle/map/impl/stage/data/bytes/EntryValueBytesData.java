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

package net.openhft.chronicle.map.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class EntryValueBytesData<V> extends AbstractData<V> {
    
    @StageRef ValueBytesInterop<V, ?, ?> vi;
    @StageRef MapEntryStages<?, V> entry;
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;


    @Stage("CachedEntryValue") private V cachedEntryValue;
    @Stage("CachedEntryValue") private boolean cachedEntryValueRead = false;

    private void initCachedEntryValue() {
        cachedEntryValue = innerGetUsing(cachedEntryValue);
        cachedEntryValueRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.entryBS;
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.valueOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.valueSize;
    }

    @Override
    public V get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedEntryValue;
    }

    @Override
    public V getUsing(V usingValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(usingValue);
    }

    private V innerGetUsing(V usingValue) {
        entry.entryBytes.position(entry.valueOffset);
        return vi.valueReader.read(entry.entryBytes, size(), usingValue);
    }
}
