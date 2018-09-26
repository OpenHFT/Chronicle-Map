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
