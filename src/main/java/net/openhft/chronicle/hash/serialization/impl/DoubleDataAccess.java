//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
public final class DoubleDataAccess extends AbstractData<Double>
        implements DataAccess<Double>, Data<Double> {

    // Cache fields
    private transient boolean bsInit;
    private transient BytesStore<?, ?> bs;

    /**
     * State field
     */
    private transient Double instance;

    public DoubleDataAccess() {
        initTransients();
    }

    private void initTransients() {
        bs = BytesStore.wrap(new byte[8]);
    }

    @Override
    public RandomDataInput bytes() {
        if (!bsInit) {
            bs.writeDouble(0, instance);
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
        return 8;
    }

    @Override
    public Double get() {
        return instance;
    }

    @Override
    public Double getUsing(@Nullable Double using) {
        return instance;
    }

    @Override
    public long hash(LongHashFunction f) {
        return f.hashLong(Double.doubleToRawLongBits(instance));
    }

    @Override
    public boolean equivalent(RandomDataInput source, long sourceOffset) {
        return source.readLong(sourceOffset) == Double.doubleToRawLongBits(instance);
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        target.writeDouble(targetOffset, instance);
    }

    @Override
    public Data<Double> getData(@NotNull Double instance) {
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
    public DataAccess<Double> copy() {
        return new DoubleDataAccess();
    }
}
