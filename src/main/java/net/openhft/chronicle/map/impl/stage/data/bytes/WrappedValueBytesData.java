/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
