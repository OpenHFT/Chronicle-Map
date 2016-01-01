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

package net.openhft.chronicle.map;

import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

import static net.openhft.lang.Maths.isPowerOf2;
import static net.openhft.lang.MemoryUnit.*;

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
        public MultiMap create(Bytes bytes, Bytes bitSetBytes) {
            return new ShortShortMultiMap(bytes, bitSetBytes);
        }
    },
    I24 {
        @Override
        public long sizeInBytes(long minCapacity) {
            return Int24Int24MultiMap.sizeInBytes(minCapacity);
        }

        @Override
        public MultiMap create(Bytes bytes, Bytes bitSetBytes) {
            return new Int24Int24MultiMap(bytes, bitSetBytes);
        }
    },
    I32 {
        @Override
        public long sizeInBytes(long minCapacity) {
            return IntIntMultiMap.sizeInBytes(minCapacity);
        }

        @Override
        public MultiMap create(Bytes bytes, Bytes bitSetBytes) {
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

    public static long sizeOfBitSetInBytes(long actualChunksPerSegment) {
        return LONGS.align(BYTES.alignAndConvert(actualChunksPerSegment, BITS), BYTES);
    }

    public static DirectBitSet newPositions(long capacity) {
        if (!isPowerOf2(capacity))
            throw new AssertionError("capacity should be a power of 2");
        capacity = LONGS.align(capacity, BITS);
        return ATSDirectBitSet.wrap(DirectStore.allocateLazy(BYTES.convert(capacity, BITS)).bytes());
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

    public abstract MultiMap create(Bytes bytes, Bytes bitSetBytes);
}
