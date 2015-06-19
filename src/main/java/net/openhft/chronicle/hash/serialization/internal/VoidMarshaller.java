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

import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;

public enum VoidMarshaller
        implements BytesInterop<Void>, BytesReader<Void>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(Void e) {
        return 0L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 0L;
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
    public boolean startsWith(Bytes bytes, Void e) {
        return false;
    }

    @Override
    public long hash(Void e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(Bytes bytes, Void e) {
        // do nothing;
    }

    @Override
    public long readSize(Bytes bytes) {
        return 0L;
    }

    @Override
    public Void read(Bytes bytes, long size) {
        // Void nothing;
        return null;
    }

    @Override
    public Void read(Bytes bytes, long size, Void toReuse) {
        return null;
    }

}
