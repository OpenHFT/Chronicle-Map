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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public enum CharArrayMarshaller implements BytesInterop<char[]>, BytesReader<char[]> {
    INSTANCE;

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull char[] chars) {
        if (bytes.capacity() - bytes.position() < chars.length * 2L)
            return false;
        long pos = bytes.position();
        for (int i = 0; i < chars.length; i++) {
            if (bytes.readChar(pos + (i * 2L)) != chars[i])
                return false;
        }
        return true;
    }

    @Override
    public boolean equivalent(@NotNull char[] a, @NotNull char[] b) {
        return Arrays.equals(a, b);
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull char[] chars) {
        return hashFunction.hashChars(chars);
    }

    @NotNull
    @Override
    public char[] read(@NotNull Bytes bytes, long size) {
        char[] chars = new char[resLen(size)];
        bytes.readFully(chars);
        return chars;
    }

    @NotNull
    @Override
    public char[] read(@NotNull Bytes bytes, long size, char[] toReuse) {
        int resLen = resLen(size);
        if (toReuse == null || toReuse.length != resLen)
            toReuse = new char[resLen];
        bytes.readFully(toReuse);
        return toReuse;
    }

    private int resLen(long size) {
        long resLen = size / 2L;
        if (resLen < 0 || resLen > Integer.MAX_VALUE)
            throw new IllegalArgumentException("char[] size should be non-negative int, " +
                    resLen + " given. Memory corruption?");
        return (int) resLen;
    }

    @Override
    public long size(@NotNull char[] chars) {
        return chars.length * 2L;
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull char[] chars) {
        bytes.write(chars);
    }
}
