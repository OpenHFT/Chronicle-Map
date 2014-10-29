/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization;

import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import sun.misc.Unsafe;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;

public enum Hasher {
    ;

    private static final long LONG_LEVEL_PRIME_MULTIPLE = 0x9ddfea08eb382d69L;
    private static final short SHORT_LEVEL_PRIME_MULTIPLE = 0x404f;
    private static final byte BYTE_LEVEL_PRIME_MULTIPLE = 0x57;

    private static final int INT_HASH_LOW_SHORT_MULTIPLE =
            nativeOrder() == LITTLE_ENDIAN ? SHORT_LEVEL_PRIME_MULTIPLE : 1;
    private static final int INT_HASH_HIGH_SHORT_MULTIPLE =
            nativeOrder() == LITTLE_ENDIAN ? 1 : SHORT_LEVEL_PRIME_MULTIPLE;

    public static long hash(Bytes bytes) {
        return hash(bytes, bytes.position(), bytes.limit());
    }

    public static long hash(Bytes bytes, long offset, long limit) {
        long h = 0;
        long i = offset;
        for (; i < limit - 7; i += 8)
            h = LONG_LEVEL_PRIME_MULTIPLE * h + bytes.readLong(i);
        for (; i < limit - 1; i += 2)
            h = SHORT_LEVEL_PRIME_MULTIPLE * h + bytes.readShort(i);
        if (i < limit)
            h = BYTE_LEVEL_PRIME_MULTIPLE * h + bytes.readByte(i);
        return hash(h);
    }

    public static long hash(Object array, int length) {
        Unsafe unsafe = NativeBytes.UNSAFE;
        long base = unsafe.arrayBaseOffset(array.getClass());
        long scale = unsafe.arrayIndexScale(array.getClass());
        long h = 0;
        long i = 0;
        long limit = ((long) length) * scale;
        for (; i < limit - 7; i += 8)
            h = LONG_LEVEL_PRIME_MULTIPLE * h + unsafe.getLong(array, base + i);
        for (; i < limit - 1; i += 2)
            h = SHORT_LEVEL_PRIME_MULTIPLE * h + unsafe.getShort(array, base + i);
        if (i < limit)
            h = BYTE_LEVEL_PRIME_MULTIPLE * h + unsafe.getByte(array, base + i);
        return hash(h);
    }

    public static long hash(int value) {
        return hash(((long) (((short) (value >>> 16)) * INT_HASH_HIGH_SHORT_MULTIPLE)) +
                ((short) value) * INT_HASH_LOW_SHORT_MULTIPLE);
    }

    public static long hash(long value) {
        return Maths.hash(value);
    }
}
