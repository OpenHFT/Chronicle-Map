/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SizedReader} for {@link StringBuilder} values encoded as UTF-8.
 *
 * <p>The reader validates the declared character count, ensures the builder has sufficient
 * capacity, and then fills it by parsing UTF-8 bytes from the supplied {@link Bytes}.
 */
public final class StringBuilderSizedReader
        implements SizedReader<StringBuilder>, EnumMarshallable<StringBuilderSizedReader> {
    public static final StringBuilderSizedReader INSTANCE = new StringBuilderSizedReader();

    private StringBuilderSizedReader() {
    }

    @NotNull
    @Override
    public StringBuilder read(Bytes<?> in, long size, @Nullable StringBuilder using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        if (using == null) {
            using = new StringBuilder(csLen);
        } else {
            using.setLength(0);
            using.ensureCapacity(csLen);
        }
        BytesUtil.parseUtf8(in, using, csLen);
        return using;
    }

    @NotNull
    @Override
    public StringBuilderSizedReader readResolve() {
        return INSTANCE;
    }
}
