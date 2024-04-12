package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;

public class BytesSizedMarshaller implements SizedReader<Bytes<?>>, SizedWriter<Bytes<?>> {
    @Override
    public Bytes<?> read(Bytes<?> in, long size, Bytes<?> using) {
        final int size0 = Maths.toInt32(size);
        if (using == null)
            using = Bytes.allocateElasticOnHeap(size0);

        in.read(using, size0);
        return using;
    }

    @Override
    public long size(Bytes<?> toWrite) {
        return toWrite.readRemaining();
    }

    @Override
    public void write(Bytes<?> out, long size, Bytes<?> toWrite) {
        out.write(toWrite, toWrite.readPosition(), size);
    }
}
