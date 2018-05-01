/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code DoubleMarshaller} is
 * deprecated in favor of {@link DoubleDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code DoubleMarshaller} is not deprecated.
 */
public final class DoubleMarshaller
        implements SizedReader<Double>, BytesReader<Double>,
        SizedWriter<Double>, BytesWriter<Double>, EnumMarshallable<DoubleMarshaller> {
    public static final DoubleMarshaller INSTANCE = new DoubleMarshaller();

    private DoubleMarshaller() {
    }

    @NotNull
    @Override
    public Double read(@NotNull Bytes in, long size, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public long size(@NotNull Double toWrite) {
        return 8L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @NotNull
    @Override
    public Double read(Bytes in, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public void write(Bytes out, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @Override
    public DoubleMarshaller readResolve() {
        return INSTANCE;
    }
}
