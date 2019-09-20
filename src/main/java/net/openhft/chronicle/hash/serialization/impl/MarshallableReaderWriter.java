/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MarshallableReaderWriter<V extends Marshallable>
        extends InstanceCreatingMarshaller<V>
        implements SizedReader<V>, SizedWriter<V> {
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
    public long size(@NotNull V toWrite) {
        Wire wire = Wires.acquireBinaryWire();
        toWrite.writeMarshallable(wire);
        return wire.bytes().readRemaining();
    }

    @Override
    public void write(Bytes out, long size, @NotNull V toWrite) {
        BinaryWire wire = Wires.binaryWireForWrite(out, out.writePosition(), size);
        toWrite.writeMarshallable(wire);
    }
}
