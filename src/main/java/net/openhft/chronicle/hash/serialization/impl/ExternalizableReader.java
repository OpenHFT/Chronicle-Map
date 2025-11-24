/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * {@link SizedReader} and {@link BytesReader} for {@link Externalizable} types.
 *
 * <p>The reader constructs or reuses an instance of the target type and delegates to its
 * {@link Externalizable#readExternal(java.io.ObjectInput)} method using an
 * {@link ObjectInputStream} backed by the supplied {@link Bytes}.
 */
public class ExternalizableReader<T extends Externalizable> extends InstanceCreatingMarshaller<T>
        implements SizedReader<T>, BytesReader<T> {

    public ExternalizableReader(Class<T> tClass) {
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
        try {
            using.readExternal(new ObjectInputStream(in.inputStream()));
            return using;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
