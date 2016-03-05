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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class ByteBufferDataAccess extends AbstractData<ByteBuffer>
        implements DataAccess<ByteBuffer> {

    // Cache fields
    private transient HeapBytesStore heapBytesStore;
    private transient NativeBytesStore nativeBytesStore;
    private transient VanillaBytes<Void> bytes;

    // State fields
    private transient ByteBuffer bb;
    private transient BytesStore bytesStore;

    public ByteBufferDataAccess() {
        initTransients();
    }

    private void initTransients() {
        heapBytesStore = HeapBytesStore.uninitialized();
        nativeBytesStore = NativeBytesStore.uninitialized();
        bytes = VanillaBytes.vanillaBytes();
    }

    @Override
    public RandomDataInput bytes() {
        return bytesStore;
    }

    @Override
    public long offset() {
        return bb.position();
    }

    @Override
    public long size() {
        return bb.remaining();
    }

    @Override
    public ByteBuffer get() {
        return bb;
    }

    @Override
    public ByteBuffer getUsing(@Nullable ByteBuffer using) {
        if (using == null || using.capacity() < bb.remaining()) {
            using = ByteBuffer.allocate(bb.remaining());
        } else {
            using.position(0);
            using.limit(bb.remaining());
        }
        bytes.bytesStore(bytesStore, bb.position(), bb.remaining());
        bytes.read(using);
        using.flip();
        return using;
    }

    @Override
    public Data<ByteBuffer> getData(@NotNull ByteBuffer instance) {
        bb = instance;
        if (instance instanceof DirectBuffer) {
            nativeBytesStore.init(instance, false);
            bytesStore = nativeBytesStore;
        } else {
            heapBytesStore.init(instance);
            bytesStore = heapBytesStore;
        }
        return this;
    }

    @Override
    public void uninit() {
        bb = null;
        if (bytesStore == nativeBytesStore) {
            nativeBytesStore.uninit();
        } else {
            heapBytesStore.uninit();
        }
    }

    @Override
    public DataAccess<ByteBuffer> copy() {
        return new ByteBufferDataAccess();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
