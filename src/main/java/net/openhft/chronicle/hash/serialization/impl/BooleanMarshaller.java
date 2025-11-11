/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class BooleanMarshaller
        implements SizedReader<Boolean>, BytesReader<Boolean>,
        SizedWriter<Boolean>, BytesWriter<Boolean>, EnumMarshallable<BooleanMarshaller> {
    public static final BooleanMarshaller INSTANCE = new BooleanMarshaller();

    private BooleanMarshaller() {
    }

    @Override
    public long size(@NotNull Boolean e) {
        return 1L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Boolean toWrite) {
        write(out, toWrite);
    }

    @NotNull
    @Override
    public Boolean read(@NotNull Bytes in, long size, Boolean using) {
        return read(in, null);
    }

    @NotNull
    @Override
    public Boolean read(Bytes<?> in, @Nullable Boolean using) {
        return in.readByte() != 0;
    }

    @Override
    public void write(Bytes<?> out, @NotNull Boolean toWrite) {
        out.writeByte((byte) (toWrite ? 'Y' : 0));
    }

    @NotNull
    @Override
    public BooleanMarshaller readResolve() {
        return INSTANCE;
    }
}
