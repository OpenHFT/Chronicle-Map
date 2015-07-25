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
