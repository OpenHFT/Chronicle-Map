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

package net.openhft.chronicle.hash.hashing;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

import static net.openhft.chronicle.hash.hashing.Primitives.*;

final class UnsafeAccess extends Access<Object> {
    public static final UnsafeAccess INSTANCE = new UnsafeAccess();

    static final Unsafe UNSAFE;
    static final long BOOLEAN_BASE;
    static final long BYTE_BASE;
    static final long CHAR_BASE;
    static final long SHORT_BASE;
    static final long INT_BASE;
    static final long LONG_BASE;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
            BOOLEAN_BASE = UNSAFE.arrayBaseOffset(boolean[].class);
            BYTE_BASE = UNSAFE.arrayBaseOffset(byte[].class);
            CHAR_BASE = UNSAFE.arrayBaseOffset(char[].class);
            SHORT_BASE = UNSAFE.arrayBaseOffset(short[].class);
            INT_BASE = UNSAFE.arrayBaseOffset(int[].class);
            LONG_BASE = UNSAFE.arrayBaseOffset(long[].class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private UnsafeAccess() {}

    @Override
    public long getLong(Object input, long offset) {
        return UNSAFE.getLong(input, offset);
    }

    @Override
    public long getUnsignedInt(Object input, long offset) {
        return unsignedInt(getInt(input, offset));
    }

    @Override
    public int getInt(Object input, long offset) {
        return UNSAFE.getInt(input, offset);
    }

    @Override
    public int getUnsignedShort(Object input, long offset) {
        return unsignedShort(getShort(input, offset));
    }

    @Override
    public int getShort(Object input, long offset) {
        return UNSAFE.getShort(input, offset);
    }

    @Override
    public int getUnsignedByte(Object input, long offset) {
        return unsignedByte(getByte(input, offset));
    }

    @Override
    public int getByte(Object input, long offset) {
        return UNSAFE.getByte(input, offset);
    }

    @Override
    public ByteOrder byteOrder(Object input) {
        return ByteOrder.nativeOrder();
    }

}
