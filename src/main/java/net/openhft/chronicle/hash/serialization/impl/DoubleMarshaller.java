/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code DoubleMarshaller} is
 * deprecated in favor of {@link DoubleDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code DoubleMarshaller} is not deprecated.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class DoubleMarshaller
        implements SizedReader<Double>, BytesReader<Double>,
        SizedWriter<Double>, BytesWriter<Double>, EnumMarshallable<DoubleMarshaller> {
    public static final DoubleMarshaller INSTANCE = new DoubleMarshaller();

    private DoubleMarshaller() {
    }

    @NotNull
    @Override
    public Double read(@NotNull Bytes in, long size, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public long size(@NotNull Double toWrite) {
        return 8L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @NotNull
    @Override
    public Double read(Bytes<?> in, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public void write(Bytes<?> out, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @NotNull
    @Override
    public DoubleMarshaller readResolve() {
        return INSTANCE;
    }
}
