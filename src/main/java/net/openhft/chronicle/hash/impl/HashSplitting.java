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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.core.Maths.isPowerOf2;

public interface HashSplitting extends Marshallable {

    static HashSplitting forSegments(int segments) {
        assert segments > 0;
        if (segments == 1)
            return ForSingleSegment.INSTANCE;
        if (isPowerOf2(segments))
            return new ForPowerOf2Segments(segments);
        return new ForNonPowerOf2Segments(segments);
    }

    int segmentIndex(long hash);

    long segmentHash(long hash);

    final class ForSingleSegment implements HashSplitting, EnumMarshallable<ForSingleSegment> {
        public static final ForSingleSegment INSTANCE = new ForSingleSegment();

        private ForSingleSegment() {
        }

        @Override
        public int segmentIndex(long hash) {
            return 0;
        }

        @Override
        public long segmentHash(long hash) {
            return hash;
        }

        @Override
        public ForSingleSegment readResolve() {
            return INSTANCE;
        }
    }

    class ForPowerOf2Segments implements HashSplitting {

        private int bits;
        private transient int mask;

        ForPowerOf2Segments(int segments) {
            assert Maths.isPowerOf2(segments);
            bits = Integer.numberOfTrailingZeros(segments);
            mask = segments - 1;
        }

        @Override
        public int segmentIndex(long hash) {
            return ((int) hash) & mask;
        }

        @Override
        public long segmentHash(long hash) {
            return hash >>> bits;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            bits = wire.read(() -> "bits").int32();
            mask = (1 << bits) - 1;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(() -> "bits").int32(bits);
        }
    }

    //TODO optimize?
    class ForNonPowerOf2Segments implements HashSplitting {

        private static final int MASK = Integer.MAX_VALUE;
        private static final int BITS = 31;

        private int segments;

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

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            segments = wire.read(() -> "segments").int32();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(() -> "segments").int32(segments);
        }
    }
}
