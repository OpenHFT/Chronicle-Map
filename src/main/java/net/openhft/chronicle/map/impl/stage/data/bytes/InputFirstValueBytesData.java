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
import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.chronicle.map.impl.stage.input.MapInputBytesValues;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class InputFirstValueBytesData<V> extends AbstractData<V> {
    
    @StageRef HashInputBytes in;
    @StageRef MapInputBytesValues values;
    @StageRef ValueBytesInterop<V, ?, ?> vi;
    
    @Stage("CachedBytesInputFirstValue") private V cachedBytesInputFirstValue;
    @Stage("CachedBytesInputFirstValue") private boolean cachedBytesInputFirstValueRead = false;
    
    private void initCachedBytesInputFirstValue() {
        cachedBytesInputFirstValue = getUsing(cachedBytesInputFirstValue);
        cachedBytesInputFirstValueRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        return in.inputStore;
    }

    @Override
    public long offset() {
        return values.firstInputValueOffset;
    }

    @Override
    public long size() {
        return values.firstInputValueSize;
    }

    @Override
    public V get() {
        return cachedBytesInputFirstValue;
    }

    @Override
    public V getUsing(V usingValue) {
        in.inputBytes.position(values.firstInputValueOffset);
        return vi.valueReader.read(in.inputBytes, size(), usingValue);
    }
}
