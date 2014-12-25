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

import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;

import static net.openhft.lang.Maths.isPowerOf2;
import static net.openhft.lang.MemoryUnit.BITS;
import static net.openhft.lang.MemoryUnit.BYTES;
import static net.openhft.lang.MemoryUnit.LONGS;

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
        public MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes) {
            return new IntIntMultiMap(bytes, bitSetBytes);
        }
    };

    public static final long I16_MAX_CAPACITY = ShortShortMultiMap.MAX_CAPACITY;
    public static final long MAX_CAPACITY = IntIntMultiMap.MAX_CAPACITY;

    public static MultiMapFactory forCapacity(long capacity) {
        if (capacity <= ShortShortMultiMap.MAX_CAPACITY)
            return I16;
        // TODO why not I24?
//        if (capacity <= Int24Int24MultiMap.MAX_CAPACITY)
//            return I24;
        if (capacity <= IntIntMultiMap.MAX_CAPACITY)
            return I32;
        throw new IllegalArgumentException("Capacity " + capacity + " not supported");
    }

    public static long sizeOfBitSetInBytes(long minCapacity) {
        return BYTES.convert(LONGS.align(multiMapCapacity(minCapacity), BITS), BITS);
    }

    public static ATSDirectBitSet newPositions(long capacity) {
        if (!isPowerOf2(capacity))
            throw new AssertionError("capacity should be a power of 2");
        capacity = LONGS.align(capacity, BITS);
        return new ATSDirectBitSet(DirectStore.allocateLazy(BYTES.convert(capacity, BITS)).bytes());
    }

    static long multiMapCapacity(long minCapacity) {
        if (minCapacity < 0L)
            throw new IllegalArgumentException("minCapacity should be positive");
        long capacity = Maths.nextPower2(minCapacity, 16L);
        if (((double) minCapacity) / capacity > 2./3.) {
            // multi map shouldn't be too dense
            capacity <<= 1L;
        }
        return capacity;
    }

    public abstract long sizeInBytes(long minCapacity);

    public abstract MultiMap create(NativeBytes bytes, NativeBytes bitSetBytes);
}
