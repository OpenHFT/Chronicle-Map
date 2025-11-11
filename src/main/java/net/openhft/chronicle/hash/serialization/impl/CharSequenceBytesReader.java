/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BytesReader} implementation for {@code CharSequence}. For the primary ChronicleMap's key
 * or value type {@link CharSequenceSizedReader} + {@link CharSequenceUtf8DataAccess} are more
 * efficient (because don't store the size twice), so this reader is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see CharSequenceBytesWriter
 */
public final class CharSequenceBytesReader implements BytesReader<CharSequence>,
        StatefulCopyable<CharSequenceBytesReader>, EnumMarshallable<CharSequenceBytesReader> {
    public static final CharSequenceBytesReader INSTANCE = new CharSequenceBytesReader();

    private CharSequenceBytesReader() {
    }

    @NotNull
    @Override
    public CharSequence read(Bytes<?> in, @Nullable CharSequence using) {
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = (StringBuilder) using;
        } else {
            usingSB = new StringBuilder();
        }
        if (in.readUtf8(usingSB)) {
            return usingSB;
        } else {
            throw new NullPointerException("BytesReader couldn't read null");
        }
    }

    @Override
    public CharSequenceBytesReader copy() {
        return INSTANCE;
    }

    @NotNull
    @Override
    public CharSequenceBytesReader readResolve() {
        return INSTANCE;
    }
}
