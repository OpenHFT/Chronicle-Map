/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

public enum DoubleMarshaller
        implements SizedReader<Double>, BytesReader<Double>,
        SizedWriter<Double>, BytesWriter<Double>, EnumMarshallable<DoubleMarshaller> {
    INSTANCE;

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
