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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.stage.input.ReplicatedInput;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;


@Staged
public class ReplicatedInputValueBytesData<V> extends AbstractData<V> {
    @StageRef ReplicatedInput<?, V, ?> in;
    @StageRef ValueBytesInterop<V> vi;
    
    @Stage("CachedBytesReplicatedInputValue") private V cachedBytesReplicatedInputValue;
    @Stage("CachedBytesReplicatedInputValue") private boolean cachedBytesReplicatedInputValueRead =
            false;
    
    private void initCachedBytesReplicatedInputValue() {
        cachedBytesReplicatedInputValue = getUsing(cachedBytesReplicatedInputValue);
        cachedBytesReplicatedInputValueRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        return in.replicatedInputBytes;
    }

    @Override
    public long offset() {
        return in.riValueOffset;
    }

    @Override
    public long size() {
        return in.riValueSize;
    }

    @Override
    public V get() {
        return cachedBytesReplicatedInputValue;
    }

    @Override
    public V getUsing(V using) {
        Bytes inputBytes = in.replicatedInputBytes;
        inputBytes.readPosition(in.riValueOffset);
        return vi.valueReader.read(inputBytes, size(), using);
    }
}
