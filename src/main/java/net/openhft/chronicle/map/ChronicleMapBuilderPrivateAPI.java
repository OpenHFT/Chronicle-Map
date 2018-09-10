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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.replication.MapRemoteOperations;

import java.util.concurrent.TimeUnit;

class ChronicleMapBuilderPrivateAPI<K, V>
        implements ChronicleHashBuilderPrivateAPI<K, MapRemoteOperations<K, V, ?>> {

    private final ChronicleMapBuilder<K, V> b;

    public ChronicleMapBuilderPrivateAPI(ChronicleMapBuilder<K, V> b) {
        this.b = b;
    }

    @Override
    public String name() {
        return b.name();
    }

    @Override
    public SerializationBuilder<K> keyBuilder() {
        return b.keyBuilder();
    }

    @Override
    public int segmentEntrySpaceInnerOffset() {
        return b.segmentEntrySpaceInnerOffset();
    }

    @Override
    public long chunkSize() {
        return b.chunkSize();
    }

    @Override
    public int maxChunksPerEntry() {
        return b.maxChunksPerEntry();
    }

    @Override
    public long entriesPerSegment() {
        return b.entriesPerSegment();
    }

    @Override
    public long tierHashLookupCapacity() {
        return b.tierHashLookupCapacity();
    }

    @Override
    public long actualChunksPerSegmentTier() {
        return b.actualChunksPerSegmentTier();
    }

    @Override
    public int segmentHeaderSize() {
        return b.segmentHeaderSize();
    }

    @Override
    public int actualSegments() {
        return b.actualSegments();
    }

    @Override
    public long maxExtraTiers() {
        return b.maxExtraTiers();
    }

    @Override
    public boolean aligned64BitMemoryOperationsAtomic() {
        return b.aligned64BitMemoryOperationsAtomic();
    }

    @Override
    public boolean checksumEntries() {
        return b.checksumEntries();
    }

    @Override
    public void replication(byte identifier) {
        b.replication(identifier);
    }

    @Override
    public void cleanupRemovedEntries(boolean cleanupRemovedEntries) {
        b.cleanupRemovedEntries(cleanupRemovedEntries);
    }

    @Override
    public void removedEntryCleanupTimeout(long removedEntryCleanupTimeout, TimeUnit unit) {
        b.removedEntryCleanupTimeout(removedEntryCleanupTimeout, unit);
    }

    @Override
    public void remoteOperations(MapRemoteOperations<K, V, ?> remoteOperations) {
        b.remoteOperations(remoteOperations);
    }

    @Override
    public Runnable getPreShutdownAction() {
        return b.preShutdownAction;
    }
}
