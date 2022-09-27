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

import net.openhft.chronicle.core.Memory;
import net.openhft.chronicle.core.OS;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;

/**
 * The reason why this is not implemented via value interface, and offsets are allocated by hand --
 * this functionality is accessed concurrently from many threads, and native value implementations
 * are stateful - so need to keep an instance in thread local that seems to be overall more pain
 * than gain.
 */
public enum TierCountersArea {
    ; // none

    public static final long NEXT_TIER_INDEX_OFFSET = 0L;
    public static final long PREV_TIER_INDEX_OFFSET = NEXT_TIER_INDEX_OFFSET + 8L;
    public static final long LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET = PREV_TIER_INDEX_OFFSET + 8L;
    public static final long SEGMENT_INDEX_OFFSET = LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET + 8L;
    public static final long TIER_OFFSET = SEGMENT_INDEX_OFFSET + 4L;
    public static final long ENTRIES_OFFSET = TIER_OFFSET + 4L;
    public static final long DELETED_OFFSET = ENTRIES_OFFSET + 4L;
    private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;
    private static final Memory MEMORY = OS.memory();

    public static long nextTierIndex(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readLong(address + NEXT_TIER_INDEX_OFFSET);
    }

    public static void nextTierIndex(final long address,
                                     final long nextTierIndex) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        MEMORY.writeLong(address + NEXT_TIER_INDEX_OFFSET, nextTierIndex);
    }

    public static long lowestPossiblyFreeChunkTiered(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readLong(address + LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET);
    }

    public static void lowestPossiblyFreeChunkTiered(final long address, final long lowestPossiblyFreeChunk) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        MEMORY.writeLong(address + LOWEST_POSSIBLY_FREE_CHUNK_TIERED_OFFSET,
                lowestPossiblyFreeChunk);
    }

    public static long prevTierIndex(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readLong(address + PREV_TIER_INDEX_OFFSET);
    }

    public static void prevTierIndex(final long address,
                                     final long prevTierIndex) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        MEMORY.writeLong(address + PREV_TIER_INDEX_OFFSET, prevTierIndex);
    }

    public static int segmentIndex(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readInt(address + SEGMENT_INDEX_OFFSET);
    }

    public static void segmentIndex(final long address, final int segmentIndex) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        MEMORY.writeInt(address + SEGMENT_INDEX_OFFSET, segmentIndex);
    }

    public static int tier(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readInt(address + TIER_OFFSET);
    }

    public static void tier(final long address,
                            final int tier) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        MEMORY.writeInt(address + TIER_OFFSET, tier);
    }

    public static long entries(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readInt(address + ENTRIES_OFFSET) & UNSIGNED_INT_MASK;
    }

    public static void entries(final long address,
                               final long entries) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (entries >= (1L << 32)) {
            throw new IllegalStateException("tier entries overflow: up to " + UNSIGNED_INT_MASK +
                    " supported, " + entries + " given");
        }
        MEMORY.writeInt(address + ENTRIES_OFFSET, (int) entries);
    }

    public static long deleted(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return MEMORY.readInt(address + DELETED_OFFSET) & UNSIGNED_INT_MASK;
    }

    public static void deleted(final long address,
                               final long deleted) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (deleted >= (1L << 32)) {
            throw new IllegalStateException("tier deleted entries count overflow: up to " +
                    UNSIGNED_INT_MASK + " supported, " + deleted + " given");
        }
        MEMORY.writeInt(address + DELETED_OFFSET, (int) deleted);
    }
}
