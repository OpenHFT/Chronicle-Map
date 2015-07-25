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
