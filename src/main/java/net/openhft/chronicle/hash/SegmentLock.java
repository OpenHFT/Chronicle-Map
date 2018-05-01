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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;

/**
 * {@link InterProcessReadWriteUpdateLock} of a segment in {@code ChronicleHash}.
 * <p>
 * <p>In Chronicle-Map off-heap design, locks (and concurrency) are per-segment.
 *
 * @see ChronicleHashBuilder#minSegments(int)
 * @see ChronicleHashBuilder#actualSegments(int)
 */
public interface SegmentLock extends InterProcessReadWriteUpdateLock {

    /**
     * Returns the index of the accessed segment.
     */
    int segmentIndex();
}
