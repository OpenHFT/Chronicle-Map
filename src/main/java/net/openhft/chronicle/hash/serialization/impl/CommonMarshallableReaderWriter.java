/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.CommonMarshallable;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A marshaller that is aware of {@link CommonMarshallable#usesSelfDescribingMessage()}
 *
 * @param <V> value class
 */
public class CommonMarshallableReaderWriter<V extends CommonMarshallable>
        extends CachingCreatingMarshaller<V> {

    public CommonMarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    @NotNull
    @Override
    public V read(Bytes<?> in, long size, @Nullable V using) {
        if (using == null)
            using = createInstance();
        if (using.usesSelfDescribingMessage()) {
            ((ReadMarshallable) using).readMarshallable(Wires.binaryWireForRead(in, in.readPosition(), size));
        } else {
            ((ReadBytesMarshallable) using).readMarshallable(in);
        }
        return using;
    }

    @Override
    protected void writeToWire(Wire wire, @NotNull V toWrite) {
        if (toWrite.usesSelfDescribingMessage()) {
            ((WriteMarshallable) toWrite).writeMarshallable(wire);
        } else {
            ((WriteBytesMarshallable) toWrite).writeMarshallable(wire.bytes());
        }
    }
}
