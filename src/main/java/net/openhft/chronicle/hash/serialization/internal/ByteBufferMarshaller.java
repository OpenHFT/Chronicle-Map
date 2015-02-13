/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public enum ByteBufferMarshaller implements BytesInterop<ByteBuffer>, BytesReader<ByteBuffer> {
    INSTANCE;

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull ByteBuffer bb) {
        int inputRemaining = bb.remaining();
        if(bytes.capacity() - bytes.position() < (long) inputRemaining)
            return false;

        long pos = bytes.position();
        int inputPos = bb.position();

        int i = 0;
        if (bytes.byteOrder() == bb.order()) {
            for (; i < inputRemaining - 7; i += 8) {
                if (bytes.readLong(pos + (long) i) != bb.getLong(inputPos + i))
                    return false;
            }
            if (i < inputRemaining - 3) {
                if (bytes.readInt(pos + (long) i) != bb.getInt(inputPos + i))
                    return false;
                i += 4;
            }
            if (i < inputRemaining - 1) {
                if (bytes.readChar(pos + (long) i) != bb.getChar(inputPos + i))
                    return false;
                i += 2;
            }
        }
        for(; i < inputRemaining; i++) {
            if (bytes.readByte(pos + (long) i) != bb.get(inputPos + i))
                return false;
        }

        return true;
    }

    @Override
    public boolean equivalent(@NotNull ByteBuffer a, @NotNull ByteBuffer b) {
        return a.equals(b);
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull ByteBuffer bb) {
        return hashFunction.hashBytes(bb);
    }

    @NotNull
    @Override
    public ByteBuffer read(@NotNull Bytes bytes, long size) {
        return read(bytes, size, null);
    }

    @NotNull
    @Override
    public ByteBuffer read(@NotNull Bytes bytes, long size, @Nullable ByteBuffer toReuse) {
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
    public long size(@NotNull ByteBuffer bb) {
        return (long) bb.remaining();
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull ByteBuffer bb) {
        int position = bb.position();
        bytes.write(bb);
        bb.position(position);
    }
}
