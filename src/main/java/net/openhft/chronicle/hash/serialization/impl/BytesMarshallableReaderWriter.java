/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BytesMarshallableReaderWriter<V extends BytesMarshallable>
        extends CachingCreatingMarshaller<V> {
    private static final ThreadLocal<VanillaBytes> VANILLA_BYTES_TL = ThreadLocal.withInitial(VanillaBytes::vanillaBytes);

    public BytesMarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    @NotNull
    @Override
    public V read(Bytes<?> in, long size, @Nullable V using) {
        if (using == null)
            using = createInstance();

        VanillaBytes vanillaBytes = VANILLA_BYTES_TL.get();
        vanillaBytes.bytesStore(in.bytesStore(), in.readPosition(), size);
        using.readMarshallable(vanillaBytes);
        return using;
    }

    @Override
    protected void writeToWire(Wire wire, @NotNull V toWrite) {
        toWrite.writeMarshallable(wire.bytes());
    }
}
