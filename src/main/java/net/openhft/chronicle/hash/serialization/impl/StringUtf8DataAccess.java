/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DataAccess} for {@link String} values encoded as UTF-8.
 *
 * <p>The access treats the wrapped string as immutable and simply returns the cached value
 * from {@link #getUsing(String)}, while exposing its UTF-8 bytes through the base class for
 * hashing and serialisation.
 */
public final class StringUtf8DataAccess extends AbstractCharSequenceUtf8DataAccess<String> {

    public StringUtf8DataAccess() {
        this(DefaultElasticBytes.DEFAULT_BYTES_CAPACITY);
    }

    private StringUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public String getUsing(@Nullable String using) {
        return cs;
    }

    @Override
    public DataAccess<String> copy() {
        return new StringUtf8DataAccess(bytes().realCapacity());
    }
}
