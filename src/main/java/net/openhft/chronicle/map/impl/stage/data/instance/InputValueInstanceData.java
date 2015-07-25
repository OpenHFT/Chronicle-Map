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
