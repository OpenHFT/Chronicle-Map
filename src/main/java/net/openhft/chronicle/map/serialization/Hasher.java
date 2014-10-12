/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map.serialization;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import sun.misc.Unsafe;

public final class Hasher {

    public static long hash(Bytes bytes) {
        return hash(bytes, bytes.position(), bytes.limit());
    }

    public static long hash(Bytes bytes, long offset, long limit) {
        long h = 0;
        long i = offset;
        for (; i < limit - 7; i += 8)
            h = 1011001110001111L * h + bytes.readLong(i);
        for (; i < limit - 1; i += 2)
            h = 101111 * h + bytes.readShort(i);
        if (i < limit)
            h = 2111 * h + bytes.readByte(i);
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
            h = 1011001110001111L * h + unsafe.getLong(array, base + i);
        for (; i < limit - 1; i += 2)
            h = 101111 * h + unsafe.getShort(array, base + i);
        if (i < limit)
            h = 2111 * h + unsafe.getByte(array, base + i);
        return hash(h);
    }

    public static long hash(long value) {
        value *= 11018881818881011L;
        value ^= (value >>> 41) ^ (value >>> 21);
        return value;
    }

    private Hasher() {}
}
