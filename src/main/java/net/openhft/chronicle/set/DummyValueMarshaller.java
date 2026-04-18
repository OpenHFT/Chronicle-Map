/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
final class DummyValueMarshaller implements DataAccess<DummyValue>, SizedReader<DummyValue>,
        EnumMarshallable<DummyValueMarshaller> {
    public static final DummyValueMarshaller INSTANCE = new DummyValueMarshaller();

    private DummyValueMarshaller() {
    }

    @Override
    public Data<DummyValue> getData(@NotNull DummyValue instance) {
        return DummyValueData.INSTANCE;
    }

    @Override
    public void uninit() {
        // no op
    }

    @Override
    public DataAccess<DummyValue> copy() {
        return this;
    }

    @NotNull
    @Override
    public DummyValue read(@NotNull Bytes in, long size, @Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }

    @NotNull
    @Override
    public DummyValueMarshaller readResolve() {
        return INSTANCE;
    }
}
