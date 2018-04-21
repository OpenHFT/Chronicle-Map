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
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class BooleanMarshaller
        implements SizedReader<Boolean>, BytesReader<Boolean>,
        SizedWriter<Boolean>, BytesWriter<Boolean>, EnumMarshallable<BooleanMarshaller> {
    public static final BooleanMarshaller INSTANCE = new BooleanMarshaller();

    private BooleanMarshaller() {
    }

    @Override
    public long size(@NotNull Boolean e) {
        return 1L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Boolean toWrite) {
        write(out, toWrite);
    }

    @NotNull
    @Override
    public Boolean read(@NotNull Bytes in, long size, Boolean using) {
        return read(in, null);
    }

    @NotNull
    @Override
    public Boolean read(Bytes in, @Nullable Boolean using) {
        return in.readByte() != 0;
    }

    @Override
    public void write(Bytes out, @NotNull Boolean toWrite) {
        out.writeByte((byte) (toWrite ? 'Y' : 0));
    }

    @Override
    public BooleanMarshaller readResolve() {
        return INSTANCE;
    }
}
