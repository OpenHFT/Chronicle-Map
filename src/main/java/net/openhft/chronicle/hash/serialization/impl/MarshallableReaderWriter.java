/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MarshallableReaderWriter<V extends Marshallable>
        extends CachingCreatingMarshaller<V> {
    public MarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    @NotNull
    @Override
    public V read(Bytes in, long size, @Nullable V using) {
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
