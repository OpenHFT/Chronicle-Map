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
