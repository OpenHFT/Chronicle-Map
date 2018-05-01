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
