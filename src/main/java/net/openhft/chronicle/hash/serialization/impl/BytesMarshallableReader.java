/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SizedReader} and {@link BytesReader} for {@link BytesMarshallable} types.
 *
 * <p>The reader creates or reuses an instance of the target type and delegates decoding to
 * its {@link BytesMarshallable#readMarshallable(Bytes)} method, so that Chronicle hash
 * structures can materialise values directly from their binary representation.
 */
public class BytesMarshallableReader<T extends BytesMarshallable>
        extends InstanceCreatingMarshaller<T> implements SizedReader<T>, BytesReader<T> {

    public BytesMarshallableReader(Class<T> tClass) {
        super(tClass);
    }

    @NotNull
    @Override
    public T read(@NotNull Bytes<?> in, long size, @Nullable T using) {
        return read(in, using);
    }

    @NotNull
    @Override
    public T read(Bytes<?> in, @Nullable T using) {
        if (using == null)
            using = createInstance();
        using.readMarshallable(in);
        return using;
    }
}
