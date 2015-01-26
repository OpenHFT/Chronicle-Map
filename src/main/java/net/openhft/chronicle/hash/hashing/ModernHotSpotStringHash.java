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

package net.openhft.chronicle.hash.hashing;

import java.lang.reflect.Field;

enum ModernHotSpotStringHash implements StringHash {
    INSTANCE;

    private static final long valueOffset;

    static {
        try {
            Field valueField = String.class.getDeclaredField("value");
            valueOffset = UnsafeAccess.UNSAFE.objectFieldOffset(valueField);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public long longHash(String s, LongHashFunction hashFunction, int off, int len) {
        char[] value = (char[]) UnsafeAccess.UNSAFE.getObject(s, valueOffset);
        return hashFunction.hashChars(value, off, len);
    }
}
