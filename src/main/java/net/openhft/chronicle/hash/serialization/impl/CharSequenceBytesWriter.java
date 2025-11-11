/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * {@link BytesWriter} implementation for {@link CharSequence}, for the primary ChronicleMap's key
 * or value type {@link CharSequenceUtf8DataAccess} + {@link CharSequenceSizedReader} are more
 * efficient (because don't store the size twice), so this writer is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see StringBytesReader
 */
public final class CharSequenceBytesWriter
        implements BytesWriter<CharSequence>, EnumMarshallable<CharSequenceBytesWriter> {
    public static final CharSequenceBytesWriter INSTANCE = new CharSequenceBytesWriter();

    private CharSequenceBytesWriter() {
    }

    @Override
    public void write(Bytes<?> out, @NotNull CharSequence toWrite) {
        requireNonNull(toWrite);
        out.writeUtf8(toWrite);
    }

    @NotNull
    @Override
    public CharSequenceBytesWriter readResolve() {
        return INSTANCE;
    }
}
