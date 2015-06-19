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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

enum ByteBufferAccess implements Access<ByteBuffer> {
    INSTANCE;

    @Override
    public long getLong(ByteBuffer input, long offset) {
        return input.getLong((int) offset);
    }

    @Override
    public long getUnsignedInt(ByteBuffer input, long offset) {
        return Primitives.unsignedInt(getInt(input, offset));
    }

    @Override
    public int getInt(ByteBuffer input, long offset) {
        return input.getInt((int) offset);
    }

    @Override
    public int getUnsignedShort(ByteBuffer input, long offset) {
        return Primitives.unsignedShort(getShort(input, offset));
    }

    @Override
    public int getShort(ByteBuffer input, long offset) {
        return input.getShort((int) offset);
    }

    @Override
    public int getUnsignedByte(ByteBuffer input, long offset) {
        return Primitives.unsignedByte(getByte(input, offset));
    }

    @Override
    public int getByte(ByteBuffer input, long offset) {
        return input.get((int) offset);
    }

    @Override
    public ByteOrder byteOrder(ByteBuffer input) {
        return input.order();
    }
}
