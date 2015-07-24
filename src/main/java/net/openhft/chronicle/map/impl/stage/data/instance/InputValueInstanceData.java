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

package net.openhft.chronicle.map.impl.stage.data.instance;

import net.openhft.chronicle.hash.impl.CopyingInstanceData;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.impl.data.instance.ValueInitializableData;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.lang.io.DirectBytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class InputValueInstanceData<V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        extends CopyingInstanceData<V> implements ValueInitializableData<V> {
    
    @StageRef ValueBytesInterop<V, VI, MVI> vi;
    
    private V value = null;
    
    @Override
    public void initValue(V value) {
        this.value = value;
    }

    @Override
    public V instance() {
        return value;
    }

    @Stage("Buffer") private DirectBytes buffer;
    @Stage("Buffer") private boolean marshalled = false;
    
    private void initBuffer() {
        MVI mvi = vi.valueMetaInterop(value);
        long size = mvi.size(vi.valueInterop, value);
        buffer = getBuffer(this.buffer, size);
        mvi.write(vi.valueInterop, buffer, value);
        buffer.flip();
        marshalled = true;
    }
    
    @Override
    public V getUsing(V usingValue) {
        buffer.position(0);
        return vi.valueReader.read(buffer, buffer.limit(), usingValue);
    }
}
