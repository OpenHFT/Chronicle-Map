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
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.lang.io.DirectBytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class WrappedValueInstanceData<
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        extends CopyingInstanceData<V> {
    
    @StageRef VanillaChronicleMapHolder<?, ?, ?, V, VI, MVI, ?> mh;
    @StageRef ValueBytesInterop<V, VI, MVI> vi;
    
    private WrappedValueInstanceData<V, VI, MVI> next;
    
    boolean nextInit() {
        return true;
    }
    
    void closeNext() {
        // do nothing
    }
    
    @Stage("Next")
    public WrappedValueInstanceData getUnusedWrappedValue() {
        if (!valueInit())
            return this;
        if (next == null)
            next = new WrappedValueInstanceData<>();
        return next.getUnusedWrappedValue();
    }
    
    private V value;
    
    public boolean valueInit() {
        return value != null;
    }
    
    public void initValue(V value) {
        mh.m().checkValue(value);
        this.value = value;
    }
    
    public void closeValue() {
        value = null;
        if (next != null)
            next.closeValue();
    }

    @Override
    public V instance() {
        return value;
    }

    @Stage("Buffer") private DirectBytes buf;
    @Stage("Buffer") private boolean marshalled = false;
    
    private void initBuffer() {
        MVI mvi = vi.valueMetaInterop(value);
        long size = mvi.size(vi.valueInterop, value);
        buf = getBuffer(this.buf, size);
        mvi.write(vi.valueInterop, buf, value);
        buf.flip();
        marshalled = true;
    }

    @Override
    public DirectBytes buffer() {
        return buf;
    }

    @Override
    public V getUsing(V usingValue) {
        buf.position(0);
        return vi.valueReader.read(buf, buf.limit(), usingValue);
    }
}
