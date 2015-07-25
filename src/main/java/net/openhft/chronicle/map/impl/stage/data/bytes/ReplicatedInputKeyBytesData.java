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
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.map.impl.stage.input.ReplicatedInput;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;


@Staged
public class ReplicatedInputKeyBytesData<K> extends AbstractData<K> {
    @StageRef ReplicatedInput<K, ?, ?> in;
    @StageRef KeyBytesInterop<K, ?, ?> ki;
    
    @Stage("CachedBytesReplicatedInputKey") private K cachedBytesReplicatedInputKey;
    @Stage("CachedBytesReplicatedInputKey") private boolean cachedBytesReplicatedInputKeyRead =
            false;
    
    private void initCachedBytesReplicatedInputKey() {
        cachedBytesReplicatedInputKey = getUsing(cachedBytesReplicatedInputKey);
        cachedBytesReplicatedInputKeyRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        return in.replicatedInputStore;
    }

    @Override
    public long offset() {
        return in.riKeyOffset;
    }

    @Override
    public long size() {
        return in.riKeySize;
    }

    @Override
    public K get() {
        return cachedBytesReplicatedInputKey;
    }

    @Override
    public K getUsing(K usingKey) {
        Bytes inputBytes = in.replicatedInputBytes;
        inputBytes.position(in.riKeyOffset);
        return ki.keyReader.read(inputBytes, size(), usingKey);
    }
}
