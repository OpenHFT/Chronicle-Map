/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl.stage.entry;

public enum ChecksumHashing {
    ;

    /**
     * A smart procedure copied from CityHash/FarmHash, see the full implementation in
     * Zero-allocation-hashing or Chronicle-Algorithms
     */
    public static long hash8To16Bytes(long len, long first8Bytes, long last8Bytes) {
        long K2 = 0x9ae16a3b2f90404fL;
        long mul = K2 + (len << 1);
        long a = first8Bytes + K2;
        long c = ((last8Bytes >>> 37) | (last8Bytes << 27)) * mul + a;
        long d = (((a >>> 25) | (a << 39)) + last8Bytes) * mul;
        long a1 = (c ^ d) * mul ^ ((c ^ d) * mul >>> 47);
        return ((d ^ a1) * mul ^ ((d ^ a1) * mul >>> 47)) * mul;
    }
}
