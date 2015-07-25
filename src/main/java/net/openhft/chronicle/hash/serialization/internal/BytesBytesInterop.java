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
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

public enum BytesBytesInterop implements BytesInterop<Bytes> {
    INSTANCE;

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull Bytes e) {
        return bytes.compare(bytes.position(), e, e.position(), e.remaining());
    }

    @Override
    public boolean equivalent(@NotNull Bytes a, @NotNull Bytes b) {
        if (a.remaining() != b.remaining())
            return false;
        return a.compare(a.position(), b, b.position(), b.remaining());
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull Bytes e) {
        return hashFunction.hashBytes(e);
    }

    @Override
    public long size(@NotNull Bytes e) {
        return e.remaining();
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull Bytes e) {
        bytes.write(e, e.position(), e.remaining());
    }
}
