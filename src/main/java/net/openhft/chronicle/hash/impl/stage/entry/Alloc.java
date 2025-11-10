//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl.stage.entry;

public interface Alloc {

    /**
     * Allocates a block of specified number of chunks in a segment tier, optionally clears the
     * previous allocation.
     *
     * @param chunks     chunks to allocate
     * @param prevPos    the previous position to clear, -1 if not needed
     * @param prevChunks the size of the previous allocation to clear, 0 if not needed
     * @return the new allocation position
     * @throws RuntimeException if fails to allocate a block
     */
    long alloc(int chunks, long prevPos, int prevChunks);
}
