/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import java.io.Serializable;

import static net.openhft.lang.Maths.isPowerOf2;

interface HashSplitting extends Serializable {

    int segmentIndex(long hash);
    long segmentHash(long hash);

    static class Splitting {
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

    static class ForPowerOf2Segments implements HashSplitting {
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
    static class ForNonPowerOf2Segments implements HashSplitting {
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
