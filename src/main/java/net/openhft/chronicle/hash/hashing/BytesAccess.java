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

package net.openhft.chronicle.hash.hashing;

import net.openhft.lang.io.Bytes;

import java.nio.ByteOrder;

enum BytesAccess implements Access<Bytes> {
    INSTANCE;

    @Override
    public long getLong(Bytes input, long offset) {
        return input.readLong(offset);
    }

    @Override
    public long getUnsignedInt(Bytes input, long offset) {
        return input.readUnsignedInt(offset);
    }

    @Override
    public int getInt(Bytes input, long offset) {
        return input.readInt(offset);
    }

    @Override
    public int getUnsignedShort(Bytes input, long offset) {
        return input.readUnsignedShort(offset);
    }

    @Override
    public int getShort(Bytes input, long offset) {
        return input.readShort(offset);
    }

    @Override
    public int getUnsignedByte(Bytes input, long offset) {
        return input.readUnsignedByte(offset);
    }

    @Override
    public int getByte(Bytes input, long offset) {
        return input.readByte(offset);
    }

    @Override
    public ByteOrder byteOrder(Bytes input) {
        return input.byteOrder();
    }

}
