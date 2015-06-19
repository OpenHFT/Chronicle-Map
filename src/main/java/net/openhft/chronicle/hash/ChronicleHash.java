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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.io.Closeable;
import java.io.File;

/**
 * This interface defines common {@link ChronicleMap} and {@link ChronicleSet}, related to off-heap
 * memory management and file-mapping. Not usable by itself.
 */
public interface ChronicleHash extends Closeable {
    /**
     * Returns the file this hash container mapped to, i. e. when it is created by
     * {@link ChronicleHashBuilder#create()} call, or {@code null} if it is purely in-memory,
     * i. e. if it is created by {@link ChronicleHashBuilder#create()} call.
     *
     * @return the file this {@link ChronicleMap} or {@link ChronicleSet} is mapped to,
     *         or {@code null} if it is not mapped to any file
     * @see ChronicleHashBuilder#createPersistedTo(java.io.File)
     */
    File file();

    /**
     * Releases the off-heap memory, used by this hash container and resources, used by replication,
     * if any. However, if hash container (hence off-heap memory, used by it) is mapped to the file
     * and there are other instances mapping the same data on the server across JVMs, the memory
     * won't be actually freed on operation system level. I. e. this method call doesn't affect
     * other {@link ChronicleMap} or {@link ChronicleSet} instances mapping the same data.
     *
     * <p>If you won't call this method, memory would be held at least until next garbage
     * collection. This could be a problem if, for example, you target rare garbage collections,
     * but load and drop {@code ChronicleHash}es regularly.
     *
     * <p>
     * TODO what about commit guarantees, when ChronicleMap is used with memory-mapped files, if
     * {@code ChronicleMap}/{@code ChronicleSet} closed/not?
     *
     * <p>After this method call behaviour of <i>all</i> methods of {@code ChronicleMap}
     * or {@code ChronicleSet} is undefined. <i>Any</i> method call on the map might throw
     * <i>any</i> exception, or even JVM crash. Shortly speaking, don't call use map closing.
     */
    @Override
    void close();
}
