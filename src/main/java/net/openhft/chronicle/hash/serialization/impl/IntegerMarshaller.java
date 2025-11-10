//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * As a writer for top-level Chronicle Map's key or value type (Integer), {@code IntegerMarshaller}
 * is deprecated in favor of {@link IntegerDataAccess_3_13}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code IntegerMarshaller} is not deprecated.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class IntegerMarshaller
        implements SizedReader<Integer>, BytesReader<Integer>,
        SizedWriter<Integer>, BytesWriter<Integer>, EnumMarshallable<IntegerMarshaller> {
    public static final IntegerMarshaller INSTANCE = new IntegerMarshaller();

    private IntegerMarshaller() {
    }

    @NotNull
    @Override
    public Integer read(@NotNull Bytes in, long size, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public long size(@NotNull Integer toWrite) {
        return 4L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @NotNull
    @Override
    public Integer read(Bytes<?> in, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public void write(Bytes<?> out, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @NotNull
    @Override
    public IntegerMarshaller readResolve() {
        return INSTANCE;
    }
}
