//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

abstract class AbstractCharSequenceUtf8DataAccess<T extends CharSequence> extends AbstractData<T>
        implements DataAccess<T>, Data<T> {

    /**
     * State field
     */
    transient T cs;
    /**
     * Cache field
     */
    private transient Bytes<?> bytes;

    AbstractCharSequenceUtf8DataAccess(long bytesCapacity) {
        initTransients(bytesCapacity);
    }

    private void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
    }

    @Override
    public Data<T> getData(T cs) {
        this.cs = Objects.requireNonNull(cs);
        bytes.clear();
        BytesUtil.appendUtf8(bytes, cs);
        return this;
    }

    @Override
    public void uninit() {
        cs = null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no config fields to read
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no config fields to write
    }

    @Override
    public RandomDataInput bytes() {
        return bytes.bytesStore();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return bytes.readRemaining();
    }

    @Override
    public T get() {
        return cs;
    }
}
