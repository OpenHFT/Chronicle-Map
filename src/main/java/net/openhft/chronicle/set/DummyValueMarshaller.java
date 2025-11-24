/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    /**
     * Singleton stateless marshaller for {@link DummyValue}.
     * <p>
     * All dummy values share the same zero-length on-the-wire representation,
     * so this implementation ignores the actual instance and always delegates
     * to {@link DummyValueData#INSTANCE} when wrapping or reading values. The
     * object is {@linkplain #readResolve() canonicalised} after deserialisation
     * to preserve singleton semantics.
     */
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
