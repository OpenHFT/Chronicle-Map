/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Allow access to the Segments that user used to make up the shared map, these methods should be
 * used with exceptional caution and were initially introduce to facilitate remote map replication.
 */
public interface SharedSegment<K, V> {

    /**
     * if passed segmentState is null, null is returned instead of readLock
     */
    AutoCloseable readLock(@Nullable VanillaChronicleMap.SegmentState segmentState);

    Map.Entry<K, V> getEntry(@NotNull VanillaChronicleMap.SegmentState segmentState, long pos);

    void readUnlock();

    void writeUnlock();

    int getIndex();

    long offsetFromPos(long pos);

    long timeStamp(long pos);

    <T> T writeLock();
}
