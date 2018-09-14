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

package net.openhft.chronicle.map.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.bytes.NoBytesStore.NO_BYTES_STORE;

@Staged
public class WrappedValueBytesData<V> extends AbstractData<V> {

    @Stage("WrappedValueBytes")
    private final VanillaBytes wrappedValueBytes =
            new VanillaBytes(NO_BYTES_STORE);
    @StageRef
    ValueBytesInterop<V> vi;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    private WrappedValueBytesData<V> next;
    @Stage("WrappedValueBytesStore")
    private BytesStore wrappedValueBytesStore;
    @Stage("WrappedValueBytesStore")
    private long wrappedValueBytesOffset;
    @Stage("WrappedValueBytesStore")
    private long wrappedValueBytesSize;
    @Stage("WrappedValueBytes")
    private boolean wrappedValueBytesUsed = false;
    @Stage("CachedWrappedValue")
    private V cachedWrappedValue;
    @Stage("CachedWrappedValue")
    private boolean cachedWrappedValueRead = false;

    boolean nextInit() {
        return true;
    }

    void closeNext() {
        // do nothing
    }

    @Stage("Next")
    public WrappedValueBytesData<V> getUnusedWrappedValueBytesData() {
        if (!wrappedValueBytesStoreInit())
            return this;
        if (next == null)
            next = new WrappedValueBytesData<>();
        return next.getUnusedWrappedValueBytesData();
    }

    boolean wrappedValueBytesStoreInit() {
        return wrappedValueBytesStore != null;
    }

    public void initWrappedValueBytesStore(BytesStore bytesStore, long offset, long size) {
        wrappedValueBytesStore = bytesStore;
        wrappedValueBytesOffset = offset;
        wrappedValueBytesSize = size;
    }

    void closeWrappedValueBytesStore() {
        wrappedValueBytesStore = null;
        if (next != null)
            next.closeWrappedValueBytesStore();
    }

    boolean wrappedValueBytesInit() {
        return wrappedValueBytesUsed;
    }

    void initWrappedValueBytes() {
        wrappedValueBytes.bytesStore(
                wrappedValueBytesStore, wrappedValueBytesOffset, wrappedValueBytesSize);
        wrappedValueBytesUsed = true;
    }

    void closeWrappedValueBytes() {
        wrappedValueBytes.bytesStore(NO_BYTES_STORE, 0, 0);
        wrappedValueBytesUsed = false;
    }

    private void initCachedWrappedValue() {
        cachedWrappedValue = innerGetUsing(cachedWrappedValue);
        cachedWrappedValueRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return wrappedValueBytes.bytesStore();
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return wrappedValueBytesOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return wrappedValueBytesSize;
    }

    @Override
    public V get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedWrappedValue;
    }

    @Override
    public V getUsing(V using) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(using);
    }

    private V innerGetUsing(V usingValue) {
        wrappedValueBytes.readPosition(wrappedValueBytesOffset);
        return vi.valueReader.read(wrappedValueBytes, wrappedValueBytesSize, usingValue);
    }
}
