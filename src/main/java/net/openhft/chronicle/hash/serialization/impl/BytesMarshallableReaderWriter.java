/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesMarshallableReaderWriter<V extends BytesMarshallable>
        extends InstanceCreatingMarshaller<V>
        implements SizedReader<V>, SizedWriter<V> {
    public BytesMarshallableReaderWriter(Class<V> vClass) {
        super(vClass);
    }

    private static final ThreadLocal<VanillaBytes> VANILLA_BYTES_TL = ThreadLocal.withInitial(() -> new VanillaBytes<>(BytesStore.empty()));

    @NotNull
    @Override
    public V read(Bytes in, long size, @Nullable V using) {
        if (using == null)
            using = createInstance();

        VanillaBytes vanillaBytes = VANILLA_BYTES_TL.get();
        vanillaBytes.bytesStore(in, in.readPosition(), size);
        using.readMarshallable(vanillaBytes);
        return using;
    }

    @Override
    public long size(@NotNull V toWrite) {
        Bytes<?> bytes = Wires.acquireBytes();
        toWrite.writeMarshallable(bytes);
        return bytes.readRemaining();
    }

    @Override
    public void write(Bytes out, long size, @NotNull V toWrite) {
        VanillaBytes vanillaBytes = VANILLA_BYTES_TL.get();
        vanillaBytes.bytesStore(out, out.readPosition(), out.readRemaining());
        vanillaBytes.writeLimit(out.writePosition() + size);
        toWrite.writeMarshallable(vanillaBytes);
    }
}
