/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferDataAccess extends AbstractData<ByteBuffer>
        implements DataAccess<ByteBuffer> {

    // Cache fields
    private transient VanillaBytes<ByteBuffer> bytes;

    // State fields
    private transient ByteBuffer bb;
    private transient BytesStore<?, ByteBuffer> bytesStore;

    public ByteBufferDataAccess() {
        initTransients();
    }

    @SuppressWarnings({"rawtype", "unchecked"})
    private void initTransients() {
        bytes = (VanillaBytes) VanillaBytes.vanillaBytes();
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
        BytesStore<?, ByteBuffer> bytesStore1 = bytesStore;
        bytes.bytesStore(bytesStore1, bb.position(), bb.remaining());
        bytes.read(using);
        using.flip();
        return using;
    }

    @Override
    public Data<ByteBuffer> getData(@NotNull ByteBuffer instance) {
        bb = instance;
        ByteOrder originalOrder = instance.order();
        bytesStore = BytesStore.follow(instance);
        instance.order(originalOrder);
        return this;
    }

    @Override
    public void uninit() {
        bb = null;
        bytesStore.release(ReferenceOwner.INIT);
        bytesStore = null;
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
