/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reader of {@link Byteable} subclasses.
 *
 * @param <T> the subtype of {@link Byteable} deserialized
 */
public class ByteableSizedReader<T extends Byteable> extends InstanceCreatingMarshaller<T>
        implements SizedReader<T> {

    public ByteableSizedReader(Class<T> tClass) {
        super(tClass);
    }

    @NotNull
    @Override
    public final T read(@NotNull Bytes<?> in, long size, @Nullable T using) {
        if (using == null)
            using = createInstance();
        using.bytesStore(in.bytesStore(), in.readPosition(), size);
        return using;
    }
}
