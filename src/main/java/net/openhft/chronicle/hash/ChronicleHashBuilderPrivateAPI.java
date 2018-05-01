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

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;

import java.util.concurrent.TimeUnit;

public interface ChronicleHashBuilderPrivateAPI<K, RO> {

    String name();

    SerializationBuilder<K> keyBuilder();

    int segmentEntrySpaceInnerOffset();

    long chunkSize();

    int maxChunksPerEntry();

    long entriesPerSegment();

    long tierHashLookupCapacity();

    long actualChunksPerSegmentTier();

    int segmentHeaderSize();

    int actualSegments();

    long maxExtraTiers();

    boolean aligned64BitMemoryOperationsAtomic();

    boolean checksumEntries();

    void replication(byte identifier);

    /**
     * Configures if replicated Chronicle Hashes, constructed by this builder, should
     * completely erase entries, removed some time ago. See {@link #removedEntryCleanupTimeout(
     *long, TimeUnit)} for more details on this mechanism.
     * <p>
     * <p>Default value is {@code true} -- old removed entries are erased with 1 second timeout.
     *
     * @param cleanupRemovedEntries if stale removed entries should be purged from Chronicle Hash
     * @return this builder back
     * @see #removedEntryCleanupTimeout(long, TimeUnit)
     * @see ReplicableEntry#doRemoveCompletely()
     */
    void cleanupRemovedEntries(boolean cleanupRemovedEntries);

    /**
     * Configures timeout after which entries, marked as removed in the Chronicle Hash, constructed
     * by this builder, are allowed to be completely removed from the data store. In replicated
     * Chronicle nodes, when {@code remove()} on the key is called, the corresponding entry
     * is not immediately erased from the data store, to let the distributed system eventually
     * converge on some value for this key (or converge on the fact, that this key is removed).
     * Chronicle Hash watch in runtime after the entries, and if one is removed and not updated
     * in any way for this {@code removedEntryCleanupTimeout}, Chronicle is allowed to remove this
     * entry completely from the data store. This timeout should depend on your distributed
     * system topology, and typical replication latencies, that should be determined experimentally.
     * <p>
     * <p>Default timeout is 1 minute.
     *
     * @param removedEntryCleanupTimeout timeout, after which stale removed entries could be erased
     *                                   from Chronicle Hash data store completely
     * @param unit                       time unit, in which the timeout is given
     * @return this builder back
     * @throws IllegalArgumentException is the specified timeout is less than 1 millisecond
     * @see #cleanupRemovedEntries(boolean)
     * @see ReplicableEntry#doRemoveCompletely()
     */
    void removedEntryCleanupTimeout(long removedEntryCleanupTimeout, TimeUnit unit);

    void remoteOperations(RO remoteOperations);
}
