//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesAsSizedReader<T>
        implements SizedReader<T>, StatefulCopyable<BytesAsSizedReader<T>> {

    /**
     * Config field
     */
    private BytesReader<T> reader;

    public BytesAsSizedReader(BytesReader<T> reader) {
        this.reader = reader;
    }

    @NotNull
    @Override
    public T read(Bytes<?> in, long size, @Nullable T using) {
        return reader.read(in, using);
    }

    @Override
    public BytesAsSizedReader<T> copy() {
        if (reader instanceof StatefulCopyable) {
            return new BytesAsSizedReader<>(StatefulCopyable.copyIfNeeded(reader));
        } else {
            return this;
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        reader = wireIn.read(() -> "reader").typedMarshallable();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "reader").typedMarshallable(reader);
    }
}
