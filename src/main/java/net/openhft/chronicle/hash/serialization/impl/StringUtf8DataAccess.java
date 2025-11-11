/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

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
