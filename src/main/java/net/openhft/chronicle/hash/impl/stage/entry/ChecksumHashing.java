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
