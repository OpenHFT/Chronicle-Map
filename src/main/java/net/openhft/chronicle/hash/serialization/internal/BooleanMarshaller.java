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
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

public enum BooleanMarshaller
        implements BytesInterop<Boolean>, BytesReader<Boolean>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(@NotNull Boolean e) {
        return 1L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 1L;
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
        assert size == 1L;
        // do nothing
    }

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull Boolean e) {
        return e == (bytes.readByte(bytes.position()) != 0);
    }

    @Override
    public boolean equivalent(@NotNull Boolean a, @NotNull Boolean b) {
        return a.booleanValue() == b.booleanValue();
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull Boolean e) {
        return hashFunction.hashByte(e ? (byte) 'Y' : 0);
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull Boolean e) {
        bytes.writeByte(e ? 'Y' : 0);
    }

    @Override
    public long readSize(Bytes bytes) {
        return 1L;
    }

    @NotNull
    @Override
    public Boolean read(@NotNull Bytes bytes, long size) {
        return bytes.readByte() != 0;
    }

    @NotNull
    @Override
    public Boolean read(@NotNull Bytes bytes, long size, Boolean toReuse) {
        return bytes.readByte() != 0;
    }
}
