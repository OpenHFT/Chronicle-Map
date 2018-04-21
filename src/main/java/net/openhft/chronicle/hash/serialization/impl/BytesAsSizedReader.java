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
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesAsSizedReader<T>
        implements SizedReader<T>, StatefulCopyable<BytesAsSizedReader<T>> {

    /**
     * Config field
     */
    private BytesReader<T> reader;

    public BytesAsSizedReader(BytesReader<T> reader) {
        this.reader = reader;
    }

    @NotNull
    @Override
    public T read(Bytes in, long size, @Nullable T using) {
        return reader.read(in, using);
    }

    @Override
    public BytesAsSizedReader<T> copy() {
        if (reader instanceof StatefulCopyable) {
            return new BytesAsSizedReader<>(StatefulCopyable.copyIfNeeded(reader));
        } else {
            return this;
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        reader = wireIn.read(() -> "reader").typedMarshallable();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "reader").typedMarshallable(reader);
    }
}
