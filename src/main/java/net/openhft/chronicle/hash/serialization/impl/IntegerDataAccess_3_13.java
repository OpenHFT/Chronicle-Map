/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.RandomDataOutput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class IntegerDataAccess_3_13 extends AbstractData<Integer>
        implements DataAccess<Integer>, Data<Integer> {

    // Cache fields
    private transient boolean bsInit;
    private transient BytesStore<?, ?> bs;

    /**
     * State field
     */
    private transient Integer instance;

    public IntegerDataAccess_3_13() {
        initTransients();
    }

    private void initTransients() {
        bs = BytesStore.wrap(new byte[4]);
    }

    @Override
    public RandomDataInput bytes() {
        if (!bsInit) {
            bs.writeInt(0, instance);
            bsInit = true;
        }
        return bs;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return 4;
    }

    @Override
    public Integer get() {
        return instance;
    }

    @Override
    public Integer getUsing(@Nullable Integer using) {
        return instance;
    }

    @Override
    public long hash(LongHashFunction f) {
        return f.hashInt(instance);
    }

    @Override
    public boolean equivalent(RandomDataInput source, long sourceOffset) {
        return source.readInt(sourceOffset) == instance;
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        target.writeInt(targetOffset, instance);
    }

    @Override
    public Data<Integer> getData(@NotNull Integer instance) {
        this.instance = instance;
        bsInit = false;
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no config fields to read
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no config fields to write
    }

    @Override
    public DataAccess<Integer> copy() {
        return new IntegerDataAccess_3_13();
    }
}
