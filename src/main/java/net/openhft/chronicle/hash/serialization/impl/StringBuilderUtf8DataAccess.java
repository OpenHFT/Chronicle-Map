/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public final class StringBuilderUtf8DataAccess
        extends AbstractCharSequenceUtf8DataAccess<StringBuilder> {

    public StringBuilderUtf8DataAccess() {
        this(DEFAULT_BYTES_CAPACITY);
    }

    private StringBuilderUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public StringBuilder getUsing(@Nullable StringBuilder using) {
        if (using != null) {
            using.setLength(0);
        } else {
            using = new StringBuilder(cs.length());
        }
        using.append(cs);
        return using;
    }

    @Override
    public DataAccess<StringBuilder> copy() {
        return new StringBuilderUtf8DataAccess(bytes().realCapacity());
    }
}
