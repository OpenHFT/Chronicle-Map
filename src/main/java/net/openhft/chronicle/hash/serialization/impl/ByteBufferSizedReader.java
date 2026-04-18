/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class ByteBufferSizedReader
        implements SizedReader<ByteBuffer>, EnumMarshallable<ByteBufferSizedReader> {
    public static final ByteBufferSizedReader INSTANCE = new ByteBufferSizedReader();

    private ByteBufferSizedReader() {
    }

    @NotNull
    @Override
    public ByteBuffer read(@NotNull Bytes in, long size, @Nullable ByteBuffer using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("ByteBuffer size should be non-negative int, " +
                    size + " given. Memory corruption?");
        int bufferCap = (int) size;
        if (using == null || using.capacity() < bufferCap) {
            using = ByteBuffer.allocate(bufferCap);
        } else {
            using.position(0);
            using.limit(bufferCap);
        }
        in.read(using);
        using.flip();
        return using;
    }

    @NotNull
    @Override
    public ByteBufferSizedReader readResolve() {
        return INSTANCE;
    }
}
