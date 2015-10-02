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

import static net.openhft.lang.io.NativeBytes.UNSAFE;

/**
 * The reason why this is not a data value generated class, and offsets are allocated by
 * hand -- this functionality is accessed concurrently from many threads, and data value generated
 * classes are stateful - so need to keep an instance in thread local that seems to be overall more
 * pain than gain.
 */
public enum TierCountersArea {
    ;

    public static final long NEXT_TIER_INDEX_OFFSET = 0L;
    public static final long PREV_TIER_INDEX_OFFSET = NEXT_TIER_INDEX_OFFSET + 8L;
    public static final long NEXT_POS_TO_SEARCH_FROM_TIERED_OFFSET = PREV_TIER_INDEX_OFFSET + 8L;
    public static final long SEGMENT_INDEX_OFFSET = NEXT_POS_TO_SEARCH_FROM_TIERED_OFFSET + 8L;
    public static final long TIER_OFFSET = SEGMENT_INDEX_OFFSET + 4L;

    public static long nextTierIndex(long address) {
        return UNSAFE.getLong(address + NEXT_TIER_INDEX_OFFSET);
    }

    public static void nextTierIndex(long address, long nextTierIndex) {
        UNSAFE.putLong(address + NEXT_TIER_INDEX_OFFSET, nextTierIndex);
    }

    public static long nextPosToSearchFromTiered(long address) {
        return UNSAFE.getLong(address + NEXT_POS_TO_SEARCH_FROM_TIERED_OFFSET);
    }

    public static void nextPosToSearchFromTiered(long address, long nextPosToSearchFrom) {
        UNSAFE.putLong(address + NEXT_POS_TO_SEARCH_FROM_TIERED_OFFSET,
                nextPosToSearchFrom);
    }

    public static long prevTierIndex(long address) {
        return UNSAFE.getLong(address + PREV_TIER_INDEX_OFFSET);
    }

    public static void prevTierIndex(long address, long prevTierIndex) {
        UNSAFE.putLong(address + PREV_TIER_INDEX_OFFSET, prevTierIndex);
    }

    public static int segmentIndex(long address) {
        return UNSAFE.getInt(address + SEGMENT_INDEX_OFFSET);
    }

    public static void segmentIndex(long address, int segmentIndex) {
        UNSAFE.putInt(address + SEGMENT_INDEX_OFFSET, segmentIndex);
    }

    public static int tier(long address) {
        return UNSAFE.getInt(address + TIER_OFFSET);
    }

    public static void tier(long address, int tier) {
        UNSAFE.putInt(address + TIER_OFFSET, tier);
    }
}
