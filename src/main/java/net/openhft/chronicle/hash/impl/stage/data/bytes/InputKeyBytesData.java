/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.bytes.NoBytesStore.NO_BYTES_STORE;

@Staged
public class InputKeyBytesData<K> extends AbstractData<K> {

    @Stage("InputKeyBytes")
    private final VanillaBytes inputKeyBytes =
            new VanillaBytes(NO_BYTES_STORE);
    @StageRef
    KeyBytesInterop<K> ki;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @Stage("InputKeyBytesStore")
    private BytesStore inputKeyBytesStore = null;
    @Stage("InputKeyBytesStore")
    private long inputKeyBytesOffset;
    @Stage("InputKeyBytesStore")
    private long inputKeyBytesSize;
    @Stage("InputKeyBytes")
    private boolean inputKeyBytesUsed = false;
    @Stage("CachedInputKey")
    private K cachedInputKey;
    @Stage("CachedInputKey")
    private boolean cachedInputKeyRead = false;

    public void initInputKeyBytesStore(BytesStore bytesStore, long offset, long size) {
        inputKeyBytesStore = bytesStore;
        inputKeyBytesOffset = offset;
        inputKeyBytesSize = size;
    }

    boolean inputKeyBytesInit() {
        return inputKeyBytesUsed;
    }

    void initInputKeyBytes() {
        inputKeyBytes.bytesStore(inputKeyBytesStore, inputKeyBytesOffset, inputKeyBytesSize);
        inputKeyBytesUsed = true;
    }

    void closeInputKeyBytes() {
        inputKeyBytes.bytesStore(NO_BYTES_STORE, 0, 0);
        inputKeyBytesUsed = false;
    }

    private void initCachedInputKey() {
        cachedInputKey = innerGetUsing(cachedInputKey);
        cachedInputKeyRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return inputKeyBytes.bytesStore();
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return inputKeyBytesOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return inputKeyBytesSize;
    }

    @Override
    public K get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedInputKey;
    }

    @Override
    public K getUsing(K using) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(using);
    }

    private K innerGetUsing(K usingKey) {
        inputKeyBytes.readPosition(inputKeyBytesOffset);
        return ki.keyReader.read(inputKeyBytes, inputKeyBytesSize, usingKey);
    }
}
