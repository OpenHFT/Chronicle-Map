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

package net.openhft.chronicle.hash.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class EntryKeyBytesData<K> extends AbstractData<K> {
    
    @StageRef KeyBytesInterop<K, ?, ?> ki;
    @StageRef HashEntryStages<K> entry;
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;


    @Stage("CachedEntryKey") private K cachedEntryKey;
    @Stage("CachedEntryKey") private boolean cachedEntryKeyRead = false;
    
    private void initCachedEntryKey() {
        cachedEntryKey = innerGetUsing(cachedEntryKey);
        cachedEntryKeyRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.entryBS;
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.keyOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.keySize;
    }

    @Override
    public K get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedEntryKey;
    }

    @Override
    public K getUsing(K usingKey) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(usingKey);
    }
    
    private K innerGetUsing(K usingKey) {
        entry.entryBytes.position(entry.keyOffset);
        return ki.keyReader.read(entry.entryBytes, size(), usingKey);
    }
}
