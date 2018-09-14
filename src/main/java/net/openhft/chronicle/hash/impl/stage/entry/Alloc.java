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
