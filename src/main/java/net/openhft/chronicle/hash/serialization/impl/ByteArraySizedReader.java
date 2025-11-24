/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SizedReader} for {@code byte[]} values stored in Chronicle hash structures.
 *
 * <p>The reader allocates or reuses an array of the requested length, validates that the
 * requested size fits into a Java array, and then reads the bytes from the supplied
 * {@link Bytes} instance. Oversized lengths are treated as indicative of memory corruption
 * and cause an exception.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ByteArraySizedReader
        implements SizedReader<byte[]>, EnumMarshallable<ByteArraySizedReader> {

    public static final ByteArraySizedReader INSTANCE = new ByteArraySizedReader();

    private ByteArraySizedReader() {
    }

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes in, long size, @Nullable byte[] using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE) {
            throw new IORuntimeException("byte[] size should be non-negative int, " +
                    size + " given. Memory corruption?");
        }
        int arrayLength = (int) size;
        if (using == null || arrayLength != using.length)
            using = new byte[arrayLength];
        in.read(using);
        return using;
    }

    @NotNull
    @Override
    public ByteArraySizedReader readResolve() {
        return INSTANCE;
    }
}
