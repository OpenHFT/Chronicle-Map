//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

public final class SizePrefixedBlob {

    public static final int HEADER_OFFSET = 0;
    public static final int SIZE_WORD_OFFSET = 8;
    public static final int SELF_BOOTSTRAPPING_HEADER_OFFSET = 12;

    public static final int READY = 0;
    public static final int NOT_COMPLETE = 0x80000000;

    public static final int DATA = 0;
    public static final int META_DATA = 0x40000000;

    public static final int SIZE_MASK = (1 << 30) - 1;

    private SizePrefixedBlob() {
    }

    public static boolean isReady(int sizeWord) {
        return sizeWord > 0;
    }

    public static int extractSize(int sizeWord) {
        return sizeWord & SIZE_MASK;
    }
}
