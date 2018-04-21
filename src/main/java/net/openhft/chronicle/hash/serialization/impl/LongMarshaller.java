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
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code LongMarshaller} is
 * deprecated in favor of {@link LongDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code LongMarshaller} is not deprecated.
 */
public final class LongMarshaller
        implements SizedReader<Long>, BytesReader<Long>,
        SizedWriter<Long>, BytesWriter<Long>, EnumMarshallable<LongMarshaller> {
    public static final LongMarshaller INSTANCE = new LongMarshaller();

    private LongMarshaller() {
    }

    @Override
    public long size(@NotNull Long toWrite) {
        return 8L;
    }

    @NotNull
    @Override
    public Long read(@NotNull Bytes in, long size, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @NotNull
    @Override
    public Long read(Bytes in, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(Bytes out, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @Override
    public LongMarshaller readResolve() {
        return INSTANCE;
    }
}
