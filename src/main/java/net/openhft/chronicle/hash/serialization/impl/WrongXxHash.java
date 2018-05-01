/*
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

/**
 * @deprecated this class exists for compatibility with old versions of Chronicle Map, which used
 * wrong version of xxHash.
 */
@Deprecated
public final class WrongXxHash {

    // xxHash constants
    private static final long P1 = -7046029288634856825L;
    private static final long P2 = -4417276706812531889L;
    private static final long P3 = 1609587929392839161L;
    private static final long P5 = 2870177450012600261L;

    private WrongXxHash() {
    }

    public static long hashInt(int input) {
        long hash = P5 + 4;
        // Preserve old, wrong behaviour. In the correct version, input is widened to long as
        // unsigned here.
        hash ^= input * P1;
        hash = Long.rotateLeft(hash, 23) * P2 + P3;
        return finalize(hash);
    }

    private static long finalize(long hash) {
        hash ^= hash >>> 33;
        hash *= P2;
        hash ^= hash >>> 29;
        hash *= P3;
        hash ^= hash >>> 32;
        return hash;
    }
}
