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

import net.openhft.chronicle.core.Memory;
import net.openhft.chronicle.core.OS;

/**
 * The reason why this is not implemented via value interface, and offsets are allocated by hand --
 * this functionality is accessed concurrently from many threads, and native value implementations
 * are stateful - so need to keep an instance in thread local that seems to be overall more pain
 * than gain.
 */
public enum TierCountersArea {
    ;

    public static final long NEXT_TIER_INDEX_OFFSET = 0L;
    public static final long PREV_TIER_INDEX_OFFSET = NEXT_TIER_INDEX_OFFSET + 8L;
    public static final long LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET = PREV_TIER_INDEX_OFFSET + 8L;
    public static final long SEGMENT_INDEX_OFFSET = LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET + 8L;
    public static final long TIER_OFFSET = SEGMENT_INDEX_OFFSET + 4L;
    public static final long ENTRIES_OFFSET = TIER_OFFSET + 4L;
    public static final long DELETED_OFFSET = ENTRIES_OFFSET + 4L;
    private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;
    private static Memory memory = OS.memory();

    public static long nextTierIndex(long address) {
        return memory.readLong(address + NEXT_TIER_INDEX_OFFSET);
    }

    public static void nextTierIndex(long address, long nextTierIndex) {
        memory.writeLong(address + NEXT_TIER_INDEX_OFFSET, nextTierIndex);
    }

    public static long lowestPossiblyFreeChunkTiered(long address) {
        return memory.readLong(address + LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET);
    }

    public static void lowestPossiblyFreeChunkTiered(long address, long lowestPossiblyFreeChunk) {
        memory.writeLong(address + LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET,
                lowestPossiblyFreeChunk);
    }

    public static long prevTierIndex(long address) {
        return memory.readLong(address + PREV_TIER_INDEX_OFFSET);
    }

    public static void prevTierIndex(long address, long prevTierIndex) {
        memory.writeLong(address + PREV_TIER_INDEX_OFFSET, prevTierIndex);
    }

    public static int segmentIndex(long address) {
        return memory.readInt(address + SEGMENT_INDEX_OFFSET);
    }

    public static void segmentIndex(long address, int segmentIndex) {
        memory.writeInt(address + SEGMENT_INDEX_OFFSET, segmentIndex);
    }

    public static int tier(long address) {
        return memory.readInt(address + TIER_OFFSET);
    }

    public static void tier(long address, int tier) {
        memory.writeInt(address + TIER_OFFSET, tier);
    }

    public static long entries(long address) {
        return memory.readInt(address + ENTRIES_OFFSET) & UNSIGNED_INT_MASK;
    }

    public static void entries(long address, long entries) {
        if (entries >= (1L << 32)) {
            throw new IllegalStateException("tier entries overflow: up to " + UNSIGNED_INT_MASK +
                    " supported, " + entries + " given");
        }
        memory.writeInt(address + ENTRIES_OFFSET, (int) entries);
    }

    public static long deleted(long address) {
        return memory.readInt(address + DELETED_OFFSET) & UNSIGNED_INT_MASK;
    }

    public static void deleted(long address, long deleted) {
        if (deleted >= (1L << 32)) {
            throw new IllegalStateException("tier deleted entries count overflow: up to " +
                    UNSIGNED_INT_MASK + " supported, " + deleted + " given");
        }
        memory.writeInt(address + DELETED_OFFSET, (int) deleted);
    }
}
