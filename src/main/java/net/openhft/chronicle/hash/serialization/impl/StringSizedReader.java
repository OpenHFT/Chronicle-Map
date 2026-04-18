/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
public class StringSizedReader implements SizedReader<String>, StatefulCopyable<StringSizedReader> {

    /**
     * Cache field
     */
    private transient StringBuilder sb;

    public StringSizedReader() {
        initTransients();
    }

    private void initTransients() {
        sb = new StringBuilder();
    }

    @NotNull
    @Override
    public String read(@NotNull Bytes in, long size, @Nullable String using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        sb.setLength(0);
        BytesUtil.parseUtf8(in, sb, (int) size);
        return sb.toString();
    }

    @Override
    public StringSizedReader copy() {
        return new StringSizedReader();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
