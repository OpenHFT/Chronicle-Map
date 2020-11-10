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
    private transient NativeBytesStore nativeBytesStore;
    private transient VanillaBytes<Void> bytes;

    // State fields
    private transient ByteBuffer bb;
    private transient BytesStore bytesStore;

    public ByteBufferDataAccess() {
        initTransients();
    }

    private void initTransients() {
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
            bytesStore = HeapBytesStore.wrap(instance);
        }
        return this;
    }

    @Override
    public void uninit() {
        bb = null;
        if (bytesStore == nativeBytesStore) {
            nativeBytesStore.uninit();
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
