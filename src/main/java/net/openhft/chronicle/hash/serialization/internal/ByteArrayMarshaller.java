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

public enum ByteArrayMarshaller implements BytesInterop<byte[]>, BytesReader<byte[]> {
    INSTANCE;

    @Override
    public long size(byte[] ba) {
        return ba.length;
    }

    @Override
    public boolean startsWith(Bytes bytes, byte[] ba) {
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
    public long hash(byte[] ba) {
        return Hasher.hash(ba);
    }

    @Override
    public void write(Bytes bytes, byte[] ba) {
        bytes.write(ba);
    }

    @Override
    public byte[] read(Bytes bytes, long size) {
        byte[] ba = new byte[resLen(size)];
        bytes.read(ba);
        return ba;
    }

    @Override
    public byte[] read(Bytes bytes, long size, byte[] toReuse) {
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
