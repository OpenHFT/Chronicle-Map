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

package net.openhft.chronicle.map;

import net.openhft.lang.io.NativeBytes;

/**
 * Created by peter on 09/11/14.
 *
 * In future we might support I32 -> I16 or similar.
 */
enum MultiMapFactory {
    I16 {
        @Override
        public long sizeInBytes(long minCapacity) {
            return ShortShortMultiMap.sizeInBytes(minCapacity);
        }

        @Override
        public long sizeOfBitSetInBytes(long minCapacity) {
            return ShortShortMultiMap.sizeOfBitSetInBytes(minCapacity);
        }

        @Override
        public MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes) {
            return new ShortShortMultiMap(bytes, bitSetBytes);
        }
    },
    I24 {
        @Override
        public long sizeInBytes(long minCapacity) {
            return Int24Int24MultiMap.sizeInBytes(minCapacity);
        }

        @Override
        public long sizeOfBitSetInBytes(long minCapacity) {
            return Int24Int24MultiMap.sizeOfBitSetInBytes(minCapacity);
        }

        @Override
        public MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes) {
            return new Int24Int24MultiMap(bytes, bitSetBytes);
        }
    },
    I32 {
        @Override
        public long sizeInBytes(long minCapacity) {
            return IntIntMultiMap.sizeInBytes(minCapacity);
        }

        @Override
        public long sizeOfBitSetInBytes(long minCapacity) {
            return IntIntMultiMap.sizeOfBitSetInBytes(minCapacity);
        }

        @Override
        public MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes) {
            return new IntIntMultiMap(bytes, bitSetBytes);
        }
    };

    public static final long I16_MAX_CAPACITY = ShortShortMultiMap.MAX_CAPACITY;
    public static final long MAX_CAPACITY = IntIntMultiMap.MAX_CAPACITY;

    public static MultiMapFactory forCapacity(long capacity) {
        if (capacity <= ShortShortMultiMap.MAX_CAPACITY)
            return I16;
//        if (capacity <= Int24Int24MultiMap.MAX_CAPACITY)
//            return I24;
        if (capacity <= IntIntMultiMap.MAX_CAPACITY)
            return I32;
        throw new IllegalArgumentException("Capacity " + capacity + " not supported");
    }

    public abstract long sizeInBytes(long minCapacity);

    public abstract long sizeOfBitSetInBytes(long minCapacity);

    public abstract MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes);
}
