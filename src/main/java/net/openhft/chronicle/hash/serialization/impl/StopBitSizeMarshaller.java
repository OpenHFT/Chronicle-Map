//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import org.jetbrains.annotations.NotNull;

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
    public void writeSize(Bytes<?> out, long sizeToWrite) {
        BytesUtil.writeStopBit(out, sizeToWrite);
    }

    @Override
    public long readSize(Bytes<?> in) {
        return BytesUtil.readStopBit(in);
    }

    @NotNull
    @Override
    public StopBitSizeMarshaller readResolve() {
        return INSTANCE;
    }
}
