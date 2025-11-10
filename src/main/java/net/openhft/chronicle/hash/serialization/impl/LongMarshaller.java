//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code LongMarshaller} is
 * deprecated in favor of {@link LongDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code LongMarshaller} is not deprecated.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class LongMarshaller
        implements SizedReader<Long>, BytesReader<Long>,
        SizedWriter<Long>, BytesWriter<Long>, EnumMarshallable<LongMarshaller> {
    public static final LongMarshaller INSTANCE = new LongMarshaller();

    private LongMarshaller() {
    }

    @Override
    public long size(@NotNull Long toWrite) {
        return 8L;
    }

    @NotNull
    @Override
    public Long read(@NotNull Bytes in, long size, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @NotNull
    @Override
    public Long read(Bytes<?> in, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(Bytes<?> out, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @NotNull
    @Override
    public LongMarshaller readResolve() {
        return INSTANCE;
    }
}
