/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link CachingCreatingMarshaller} for Chronicle {@link Marshallable} types.
 *
 * <p>Values are read and written using {@link Wire}-level marshalling, which allows complex
 * object graphs to be stored in Chronicle hash structures while still benefiting from
 * cached size computations for repeated values.
 */
public class MarshallableReaderWriter<V extends Marshallable>
        extends CachingCreatingMarshaller<V> {
    public MarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    @NotNull
    @Override
    public V read(Bytes<?> in, long size, @Nullable V using) {
        if (using == null)
            using = createInstance();

        using.readMarshallable(Wires.binaryWireForRead(in, in.readPosition(), size));
        return using;
    }

    @Override
    protected void writeToWire(Wire wire, @NotNull V toWrite) {
        toWrite.writeMarshallable(wire);
    }
}
