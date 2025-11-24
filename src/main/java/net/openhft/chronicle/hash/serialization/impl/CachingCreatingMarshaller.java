/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for marshallers that encode values via Chronicle Wire with per-thread caching.
 *
 * <p>The marshaller uses a thread-local {@link Wire} to compute the size of a value and,
 * when possible, reuses the previously encoded bytes when writing to an output {@link Bytes}.
 * Subclasses implement {@link #writeToWire(Wire, Object)} to define how a particular value
 * type is written.
 */
public abstract class CachingCreatingMarshaller<V>
        extends InstanceCreatingMarshaller<V>
        implements SizedReader<V>, SizedWriter<V> {

    static final ThreadLocal<Wire> WIRE_TL = ThreadLocal.withInitial(
            () -> WireType.BINARY_LIGHT.apply(Bytes.allocateElasticOnHeap(128)));
    static final ThreadLocal<Object> LAST_TL = new ThreadLocal<>();

    public CachingCreatingMarshaller(Class<V> vClass) {
        super(vClass);
    }

    @Override
    public long size(@NotNull V toWrite) {
        Wire wire = WIRE_TL.get();
        wire.bytes().clear();
        writeToWire(wire, toWrite);
        LAST_TL.set(toWrite);
        return wire.bytes().readRemaining();
    }

    protected abstract void writeToWire(Wire wire, @NotNull V toWrite);

    @Override
    public void write(Bytes<?> out, long size, @NotNull V toWrite) {
        if (LAST_TL.get() == toWrite) {
            Wire wire = WIRE_TL.get();
            if (wire.bytes().readRemaining() == size) {
                out.write(wire.bytes());
                wire.bytes().clear();
                LAST_TL.remove();
                return;
            }
        }
        BinaryWire wire = Wires.binaryWireForWrite(out, out.writePosition(), size);
        writeToWire(wire, toWrite);
    }
}
