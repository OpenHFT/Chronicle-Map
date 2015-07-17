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
