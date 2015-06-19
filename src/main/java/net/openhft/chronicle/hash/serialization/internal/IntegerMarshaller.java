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
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;

public enum IntegerMarshaller
        implements BytesInterop<Integer>, BytesReader<Integer>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(Integer e) {
        return 4L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 4L;
    }

    @Override
    public int minSizeEncodingSize() {
        return 0;
    }

    @Override
    public int maxSizeEncodingSize() {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        // do nothing
    }

    @Override
    public boolean startsWith(Bytes bytes, Integer e) {
        return e == bytes.readInt(bytes.position());
    }

    @Override
    public long hash(Integer e) {
        return Hasher.hash(e);
    }

    @Override
    public void write(Bytes bytes, Integer e) {
        bytes.writeInt(e);
    }

    @Override
    public long readSize(Bytes bytes) {
        return 4L;
    }

    @Override
    public Integer read(Bytes bytes, long size) {
        return bytes.readInt();
    }

    @Override
    public Integer read(Bytes bytes, long size, Integer toReuse) {
        return bytes.readInt();
    }
}
