//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

public final class ConstantSizeMarshaller implements SizeMarshaller {

    /**
     * Config field
     */
    private long constantSize;

    public ConstantSizeMarshaller(long constantSize) {
        this.constantSize = constantSize;
    }

    @Override
    public int storingLength(long size) {
        return 0;
    }

    @Override
    public long minStorableSize() {
        return constantSize;
    }

    @Override
    public long maxStorableSize() {
        return constantSize;
    }

    @Override
    public int minStoringLengthOfSizesInRange(long minSize, long maxSize) {
        return 0;
    }

    @Override
    public int maxStoringLengthOfSizesInRange(long minSize, long maxSize) {
        return 0;
    }

    @Override
    public void writeSize(Bytes<?> out, long sizeToWrite) {
        if (sizeToWrite != constantSize) {
            throw new IllegalArgumentException(
                    "sizeToWrite: " + sizeToWrite + ", constant size should be: " + constantSize);
        }
        // do nothing
    }

    @Override
    public long readSize(Bytes<?> in) {
        return constantSize;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        constantSize = wireIn.read(() -> "constantSize").int64();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "constantSize").int64(constantSize);
    }
}
