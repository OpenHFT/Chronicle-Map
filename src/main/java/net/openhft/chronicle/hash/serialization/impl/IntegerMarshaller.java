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
 * As a writer for top-level Chronicle Map's key or value type (Integer), {@code IntegerMarshaller}
 * is deprecated in favor of {@link IntegerDataAccess_3_13}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code IntegerMarshaller} is not deprecated.
 */
public final class IntegerMarshaller
        implements SizedReader<Integer>, BytesReader<Integer>,
        SizedWriter<Integer>, BytesWriter<Integer>, EnumMarshallable<IntegerMarshaller> {
    public static final IntegerMarshaller INSTANCE = new IntegerMarshaller();

    private IntegerMarshaller() {
    }

    @NotNull
    @Override
    public Integer read(@NotNull Bytes in, long size, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public long size(@NotNull Integer toWrite) {
        return 4L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @NotNull
    @Override
    public Integer read(Bytes in, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public void write(Bytes out, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @Override
    public IntegerMarshaller readResolve() {
        return INSTANCE;
    }
}
