/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * {@link SizedReader} and {@link BytesReader} implementation for {@link Serializable} types.
 *
 * <p>The reader reconstructs objects from a Java {@link ObjectInputStream} backed by the
 * supplied {@link Bytes}, providing a fallback for types without Chronicle-specific
 * marshallers.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SerializableReader<T extends Serializable> implements SizedReader<T>, BytesReader<T> {

    @NotNull
    @Override
    public T read(@NotNull Bytes<?> in, long size, @Nullable T using) {
        return read(in, using);
    }

    @NotNull
    @Override
    public T read(Bytes<?> in, @Nullable T using) {
        try {
            return (T) new ObjectInputStream(in.inputStream()).readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
