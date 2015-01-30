/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import java.util.Arrays;

public enum ByteArrayMarshaller implements BytesInterop<byte[]>, BytesReader<byte[]> {
    INSTANCE;

    @Override
    public long size(@NotNull byte[] ba) {
        return ba.length;
    }

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull byte[] ba) {
        if (bytes.capacity() - bytes.position() < (long) ba.length)
            return false;
        long pos = bytes.position();
        for (int i = 0; i < ba.length; i++) {
            if (bytes.readByte(pos + i) != ba[i])
                return false;
        }
        return true;
    }

    @Override
    public boolean equivalent(@NotNull byte[] a, @NotNull byte[] b) {
        return Arrays.equals(a, b);
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull byte[] ba) {
        return hashFunction.hashBytes(ba);
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull byte[] ba) {
        bytes.write(ba);
    }

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes bytes, long size) {
        byte[] ba = new byte[resLen(size)];
        bytes.read(ba);
        return ba;
    }

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes bytes, long size, byte[] toReuse) {
        int resLen = resLen(size);
        if (toReuse == null || resLen != toReuse.length)
            toReuse = new byte[resLen];
        bytes.read(toReuse);
        return toReuse;
    }

    private int resLen(long size) {
        if (size < 0L || size > (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("byte[] size should be non-negative int, " +
                    size + " given. Memory corruption?");
        return (int) size;
    }
}
