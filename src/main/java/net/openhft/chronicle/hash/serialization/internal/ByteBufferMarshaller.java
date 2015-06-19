/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.Accesses;
import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public enum ByteBufferMarshaller implements BytesInterop<ByteBuffer>, BytesReader<ByteBuffer> {
    INSTANCE;

    @Override
    public boolean startsWith(Bytes bytes, ByteBuffer bb) {
        int inputRemaining = bb.remaining();
        if(bytes.capacity() - bytes.position() < (long) inputRemaining)
            return false;

        long pos = bytes.position();
        int inputPos = bb.position();

        int i;
        for(i = 0; i < inputRemaining - 7; i += 8) {
            if (bytes.readLong(pos + (long) i) != bb.getLong(inputPos + i))
                return false;
        }
        for(; i < inputRemaining - 3; i += 4) {
            if (bytes.readInt(pos + (long) i) != bb.getInt(inputPos + i))
                return false;
        }
        for(; i < inputRemaining; i++) {
            if (bytes.readByte(pos + (long) i) != bb.get(inputPos + i))
                return false;
        }

        return true;
    }

    @Override
    public long hash(ByteBuffer bb) {
        return Hasher.hash(bb, Accesses.toByteBuffer(), 0L, (long) bb.remaining());
    }

    @Override
    public ByteBuffer read(Bytes bytes, long size) {
        return read(bytes, size, null);
    }

    @Override
    public ByteBuffer read(Bytes bytes, long size, @Nullable ByteBuffer toReuse) {
        if (size < 0L || size > (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("ByteBuffer size should be non-negative int, " +
                    size + " given. Memory corruption?");
        int intSize = (int) size;
        if (toReuse == null || toReuse.capacity() < intSize) {
            toReuse = ByteBuffer.allocate(intSize);
        } else {
            toReuse.position(0);
            toReuse.limit(intSize);
        }
        bytes.read(toReuse);
        toReuse.flip();
        return toReuse;
    }

    @Override
    public long size(ByteBuffer bb) {
        return (long) bb.remaining();
    }

    @Override
    public void write(Bytes bytes, ByteBuffer bb) {
        int position = bb.position();
        bytes.write(bb);
        bb.position(position);
    }
}
