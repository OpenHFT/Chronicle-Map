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

import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;

public enum CharArrayMarshaller implements BytesInterop<char[]>, BytesReader<char[]> {
    INSTANCE;

    @Override
    public boolean startsWith(Bytes bytes, char[] chars) {
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
    public long hash(char[] chars) {
        return Hasher.hash(chars);
    }

    @Override
    public char[] read(Bytes bytes, long size) {
        char[] chars = new char[resLen(size)];
        bytes.readFully(chars);
        return chars;
    }

    @Override
    public char[] read(Bytes bytes, long size, char[] toReuse) {
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
    public long size(char[] chars) {
        return chars.length * 2L;
    }

    @Override
    public void write(Bytes bytes, char[] chars) {
        bytes.write(chars);
    }
}
