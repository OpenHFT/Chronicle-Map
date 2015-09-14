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

import static java.lang.Double.doubleToLongBits;

public enum DoubleMarshaller implements BytesInterop<Double>, BytesReader<Double>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(Double e) {
        return 8L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 8L;
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
    public boolean startsWith(Bytes bytes, Double e) {
        return doubleToLongBits(e) == bytes.readLong(bytes.position());
    }

    @Override
    public long hash(Double e) {
        return Hasher.hash(doubleToLongBits(e));
    }

    @Override
    public void write(Bytes bytes, Double e) {
        bytes.writeLong(doubleToLongBits(e));
    }

    @Override
    public long readSize(Bytes bytes) {
        return 8L;
    }

    @Override
    public Double read(Bytes bytes, long size) {
        return Double.longBitsToDouble(bytes.readLong());
    }

    @Override
    public Double read(Bytes bytes, long size, Double toReuse) {
        return read(bytes, size);
    }
}
