/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;

import static java.lang.Math.max;
import static java.lang.Math.min;

public final class StopBitSizeMarshaller
        implements SizeMarshaller, EnumMarshallable<StopBitSizeMarshaller> {
    public static final StopBitSizeMarshaller INSTANCE = new StopBitSizeMarshaller();
    private static final long MIN_ENCODABLE_SIZE = Long.MIN_VALUE;
    private static final long MAX_ENCODABLE_SIZE = Long.MAX_VALUE;
    private StopBitSizeMarshaller() {
    }

    private static void rangeChecks(long minSize, long maxSize) {
        if (minSize > maxSize)
            throw new IllegalArgumentException("minSize = " + minSize + ", maxSize = " + maxSize);
    }

    @Override
    public int storingLength(long size) {
        return BytesUtil.stopBitLength(size);
    }

    @Override
    public long minStorableSize() {
        return MIN_ENCODABLE_SIZE;
    }

    @Override
    public long maxStorableSize() {
        return MAX_ENCODABLE_SIZE;
    }

    @Override
    public int minStoringLengthOfSizesInRange(long minSize, long maxSize) {
        rangeChecks(minSize, maxSize);
        // different signs
        if (minSize * maxSize < 0) {
            // the range includes 0 which encoding length is 1
            return 1;
        }
        return min(storingLength(minSize), storingLength(maxSize));
    }

    @Override
    public int maxStoringLengthOfSizesInRange(long minSize, long maxSize) {
        rangeChecks(minSize, maxSize);
        return max(storingLength(minSize), storingLength(maxSize));
    }

    @Override
    public void writeSize(Bytes out, long sizeToWrite) {
        BytesUtil.writeStopBit(out, sizeToWrite);
    }

    @Override
    public long readSize(Bytes in) {
        return BytesUtil.readStopBit(in);
    }

    @Override
    public StopBitSizeMarshaller readResolve() {
        return INSTANCE;
    }
}
