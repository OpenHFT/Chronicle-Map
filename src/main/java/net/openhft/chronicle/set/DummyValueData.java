/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.stage.data.ZeroBytesStore;
import org.jetbrains.annotations.Nullable;

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
