/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.hash.impl;

import java.io.Serializable;

import static net.openhft.chronicle.core.Maths.isPowerOf2;

public interface HashSplitting extends Serializable {

    int segmentIndex(long hash);
    long segmentHash(long hash);

    class Splitting {
        static HashSplitting forSegments(int segments) {
            assert segments > 0;
            if (segments == 1)
                return ForSingleSegment.INSTANCE;
            if (isPowerOf2(segments))
                return new ForPowerOf2Segments(segments);
            return new ForNonPowerOf2Segments(segments);
        }
    }

    enum ForSingleSegment implements HashSplitting {
        INSTANCE;

        @Override
        public int segmentIndex(long hash) {
            return 0;
        }

        @Override
        public long segmentHash(long hash) {
            return hash;
        }
    }

    class ForPowerOf2Segments implements HashSplitting {
        private static final long serialVersionUID = 0L;

        private final int mask;
        private final int bits;

        ForPowerOf2Segments(int segments) {
            mask = segments - 1;
            bits = Integer.numberOfTrailingZeros(segments);
        }

        @Override
        public int segmentIndex(long hash) {
            return ((int) hash) & mask;
        }

        @Override
        public long segmentHash(long hash) {
            return hash >>> bits;
        }
    }

    //TODO optimize?
    class ForNonPowerOf2Segments implements HashSplitting {
        private static final long serialVersionUID = 0L;

        private static final int MASK = Integer.MAX_VALUE;
        private static final int BITS = 31;

        private final int segments;

        public ForNonPowerOf2Segments(int segments) {
            this.segments = segments;
        }

        @Override
        public int segmentIndex(long hash) {
            return (((int) hash) & MASK) % segments;
        }

        @Override
        public long segmentHash(long hash) {
            return hash >>> BITS;
        }
    }
}
