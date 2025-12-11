/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

/**
 * Hashing helpers for computing checksums over small byte ranges.
 */
public enum ChecksumHashing {
    ; // none

    /**
     * A smart procedure copied from CityHash/FarmHash, see the full implementation in
     * Zero-allocation-hashing or Chronicle-Algorithms
     *
     * @param len         length in bytes
     * @param first8Bytes first 8 bytes as long
     * @param last8Bytes  last 8 bytes as long
     * @return 64-bit hash value
     */
    public static long hash8To16Bytes(long len, long first8Bytes, long last8Bytes) {
        long k2 = 0x9ae16a3b2f90404fL;
        long mul = k2 + (len << 1);
        long a = first8Bytes + k2;
        long c = ((last8Bytes >>> 37) | (last8Bytes << 27)) * mul + a;
        long d = (((a >>> 25) | (a << 39)) + last8Bytes) * mul;
        long a1 = (c ^ d) * mul ^ ((c ^ d) * mul >>> 47);
        return ((d ^ a1) * mul ^ ((d ^ a1) * mul >>> 47)) * mul;
    }
}
