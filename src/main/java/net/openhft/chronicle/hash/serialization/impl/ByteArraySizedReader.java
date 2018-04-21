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
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ByteArraySizedReader
        implements SizedReader<byte[]>, EnumMarshallable<ByteArraySizedReader> {

    public static final ByteArraySizedReader INSTANCE = new ByteArraySizedReader();

    private ByteArraySizedReader() {
    }

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes in, long size, @Nullable byte[] using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE) {
            throw new IORuntimeException("byte[] size should be non-negative int, " +
                    size + " given. Memory corruption?");
        }
        int arrayLength = (int) size;
        if (using == null || arrayLength != using.length)
            using = new byte[arrayLength];
        in.read(using);
        return using;
    }

    @Override
    public ByteArraySizedReader readResolve() {
        return INSTANCE;
    }
}
