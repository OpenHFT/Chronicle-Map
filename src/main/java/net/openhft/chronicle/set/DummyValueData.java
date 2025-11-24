/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.stage.data.ZeroBytesStore;
import org.jetbrains.annotations.Nullable;

/**
 * {@link net.openhft.chronicle.hash.Data} implementation used as the value
 * representation for {@link DummyValue}-backed Chronicle-Set views.
 * <p>
 * The bytes view is backed by {@link ZeroBytesStore} and always reports a
 * fixed zero-length region, while {@link #get()} and
 * {@link #getUsing(DummyValue)} return the singleton
 * {@link DummyValue#DUMMY_VALUE}. This avoids allocating per-entry value
 * objects while still satisfying the Chronicle-Hash data contract.
 */
public class DummyValueData extends AbstractData<DummyValue> {

    public static final DummyValueData INSTANCE = new DummyValueData();

    private DummyValueData() {
    }

    @Override
    public RandomDataInput bytes() {
        return ZeroBytesStore.INSTANCE;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public DummyValue get() {
        return DummyValue.DUMMY_VALUE;
    }

    @Override
    public DummyValue getUsing(@Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }
}
