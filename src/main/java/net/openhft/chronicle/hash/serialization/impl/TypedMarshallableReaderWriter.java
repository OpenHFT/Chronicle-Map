/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
public class TypedMarshallableReaderWriter<V extends Marshallable>
        extends CachingCreatingMarshaller<V> {

    public TypedMarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    @NotNull
    @Override
    public V read(Bytes<?> in, long size, @Nullable V using) {
        BinaryWire wire = Wires.binaryWireForRead(in, in.readPosition(), size);
        return wire.getValueIn().object(using, tClass());
    }

    protected void writeToWire(Wire wire, @NotNull V toWrite) {
        wire.getValueOut().object(toWrite);
    }
}
