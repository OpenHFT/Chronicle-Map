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
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class ByteBufferSizedReader
        implements SizedReader<ByteBuffer>, EnumMarshallable<ByteBufferSizedReader> {
    public static final ByteBufferSizedReader INSTANCE = new ByteBufferSizedReader();

    private ByteBufferSizedReader() {
    }

    @NotNull
    @Override
    public ByteBuffer read(@NotNull Bytes in, long size, @Nullable ByteBuffer using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("ByteBuffer size should be non-negative int, " +
                    size + " given. Memory corruption?");
        int bufferCap = (int) size;
        if (using == null || using.capacity() < bufferCap) {
            using = ByteBuffer.allocate(bufferCap);
        } else {
            using.position(0);
            using.limit(bufferCap);
        }
        in.read(using);
        using.flip();
        return using;
    }

    @Override
    public ByteBufferSizedReader readResolve() {
        return INSTANCE;
    }
}
