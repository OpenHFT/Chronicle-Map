/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

public class ByteArrayDataAccess extends AbstractData<byte[]> implements DataAccess<byte[]> {

    /**
     * Cache field
     */
    private transient BytesStore<?, ?> bs;

    /**
     * State field
     */
    private transient byte[] array;

    public ByteArrayDataAccess() {
        initTransients();
    }

    private void initTransients() {
        bs = null;
    }

    @Override
    public RandomDataInput bytes() {
        return bs;
    }

    @Override
    public long offset() {
        return bs.start();
    }

    @Override
    public long size() {
        return bs.capacity();
    }

    @Override
    public byte[] get() {
        return array;
    }

    @Override
    public byte[] getUsing(@Nullable byte[] using) {
        if (using == null || using.length != array.length)
            using = new byte[array.length];
        System.arraycopy(array, 0, using, 0, array.length);
        return using;
    }

    @Override
    public Data<byte[]> getData(@NotNull byte[] instance) {
        array = instance;
        bs = BytesStore.wrap(array);
        return this;
    }

    @Override
    public void uninit() {
        array = null;
        bs = null;
    }

    @Override
    public DataAccess<byte[]> copy() {
        return new ByteArrayDataAccess();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }

    @Override
    public String toString() {
        return new String(array, StandardCharsets.UTF_8);
    }
}
