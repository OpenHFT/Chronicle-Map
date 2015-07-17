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

package net.openhft.chronicle.hash.impl.stage.data.instance;

import net.openhft.chronicle.hash.impl.CopyingInstanceData;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableData;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.lang.io.DirectBytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class InputKeyInstanceData<K, KI, MKI extends MetaBytesInterop<K, ? super KI>>
        extends CopyingInstanceData<K> implements KeyInitableData<K> {
    
    @StageRef KeyBytesInterop<K, KI, MKI> ki;
    
    private K key = null;
    
    @Override
    public void initKey(K key) {
        this.key = key;
    }

    @Override
    public K instance() {
        return key;
    }

    @Stage("Buffer") private DirectBytes buffer;
    @Stage("Buffer") private boolean marshalled = false;
    
    private void initBuffer() {
        MKI mki = ki.keyMetaInterop(key);
        long size = mki.size(ki.keyInterop, key);
        buffer = getBuffer(this.buffer, size);
        mki.write(ki.keyInterop, buffer, key);
        buffer.flip();
        marshalled = true;
    }

    @Override
    public K getUsing(K usingKey) {
        buffer.position(0);
        return ki.keyReader.read(buffer, buffer.limit(), usingKey);
    }
}
