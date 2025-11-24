/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DataAccess} for general {@link CharSequence} values encoded as UTF-8.
 *
 * <p>The access reuses a backing {@link StringBuilder} when copying out via
 * {@link #getUsing(CharSequence)}, allowing Chronicle hash structures to work with a
 * mutable view when desired while still storing a compact UTF-8 representation.
 */
public final class CharSequenceUtf8DataAccess
        extends AbstractCharSequenceUtf8DataAccess<CharSequence> {

    public CharSequenceUtf8DataAccess() {
        this(DefaultElasticBytes.DEFAULT_BYTES_CAPACITY);
    }

    private CharSequenceUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public CharSequence getUsing(@Nullable CharSequence using) {
        StringBuilder sb;
        if (using instanceof StringBuilder) {
            sb = (StringBuilder) using;
            sb.setLength(0);
        } else {
            sb = new StringBuilder(cs.length());
        }
        sb.append(cs);
        return sb;
    }

    @Override
    public DataAccess<CharSequence> copy() {
        return new CharSequenceUtf8DataAccess(bytes().realCapacity());
    }
}
