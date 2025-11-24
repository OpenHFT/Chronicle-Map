/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SizedReader} for {@link CharSequence} values encoded as UTF-8.
 *
 * <p>The reader validates the declared character length, then parses UTF-8 bytes from the
 * supplied {@link Bytes} into a {@link StringBuilder}, reusing the {@code using} instance
 * when possible to reduce allocations.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class CharSequenceSizedReader implements SizedReader<CharSequence>,
        StatefulCopyable<CharSequenceSizedReader>, ReadResolvable<CharSequenceSizedReader> {

    public static final CharSequenceSizedReader INSTANCE = new CharSequenceSizedReader();

    private CharSequenceSizedReader() {
    }

    @NotNull
    @Override
    public CharSequence read(
            @NotNull Bytes in, long size, @Nullable CharSequence using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = ((StringBuilder) using);
            usingSB.setLength(0);
            usingSB.ensureCapacity(csLen);
        } else {
            usingSB = new StringBuilder(csLen);
        }
        BytesUtil.parseUtf8(in, usingSB, csLen);
        return usingSB;
    }

    @Override
    public CharSequenceSizedReader copy() {
        return INSTANCE;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @NotNull
    @Override
    public CharSequenceSizedReader readResolve() {
        return INSTANCE;
    }
}
