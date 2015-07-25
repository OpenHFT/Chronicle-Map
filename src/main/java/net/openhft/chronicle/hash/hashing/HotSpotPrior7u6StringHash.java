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

import java.lang.reflect.Field;

enum HotSpotPrior7u6StringHash implements StringHash {
    INSTANCE;

    private static final long valueOffset;
    private static final long offsetOffset;

    static {
        try {
            Field valueField = String.class.getDeclaredField("value");
            valueOffset = UnsafeAccess.UNSAFE.objectFieldOffset(valueField);

            Field offsetField = String.class.getDeclaredField("offset");
            offsetOffset = UnsafeAccess.UNSAFE.objectFieldOffset(offsetField);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public long longHash(String s, LongHashFunction hashFunction, int off, int len) {
        char[] value = (char[]) UnsafeAccess.UNSAFE.getObject(s, valueOffset);
        int offset = UnsafeAccess.UNSAFE.getInt(s, offsetOffset);
        return hashFunction.hashChars(value, offset + off, len);
    }
}
