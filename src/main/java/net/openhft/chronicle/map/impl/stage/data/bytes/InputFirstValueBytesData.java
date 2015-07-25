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
