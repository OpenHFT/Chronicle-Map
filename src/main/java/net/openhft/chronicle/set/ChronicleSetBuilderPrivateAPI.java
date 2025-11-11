/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.set.replication.SetRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteQueryContext;

import java.util.concurrent.TimeUnit;

@SuppressWarnings({"rawtypes", "unchecked"})
class ChronicleSetBuilderPrivateAPI<K>
        implements ChronicleHashBuilderPrivateAPI<K, SetRemoteOperations<K, ?>> {

    private final ChronicleHashBuilderPrivateAPI<K, MapRemoteOperations<K, DummyValue, ?>> mapB;

    public ChronicleSetBuilderPrivateAPI(
            ChronicleHashBuilderPrivateAPI<K, MapRemoteOperations<K, DummyValue, ?>> mapB) {
        this.mapB = mapB;
    }

    @Override
    public String name() {
        return mapB.name();
    }

    @Override
    public SerializationBuilder<K> keyBuilder() {
        return mapB.keyBuilder();
    }

    @Override
    public int segmentEntrySpaceInnerOffset() {
        return mapB.segmentEntrySpaceInnerOffset();
    }

    @Override
    public long chunkSize() {
        return mapB.chunkSize();
    }

    @Override
    public int maxChunksPerEntry() {
        return mapB.maxChunksPerEntry();
    }

    @Override
    public long entriesPerSegment() {
        return mapB.entriesPerSegment();
    }

    @Override
    public long tierHashLookupCapacity() {
        return mapB.tierHashLookupCapacity();
    }

    @Override
    public long actualChunksPerSegmentTier() {
        return mapB.actualChunksPerSegmentTier();
    }

    @Override
    public int segmentHeaderSize() {
        return mapB.segmentHeaderSize();
    }

    @Override
    public int actualSegments() {
        return mapB.actualSegments();
    }

    @Override
    public long maxExtraTiers() {
        return mapB.maxExtraTiers();
    }

    @Override
    public boolean aligned64BitMemoryOperationsAtomic() {
        return mapB.aligned64BitMemoryOperationsAtomic();
    }

    @Override
    public boolean checksumEntries() {
        return mapB.checksumEntries();
    }

    @Override
    public void replication(byte identifier) {
        mapB.replication(identifier);
    }

    @Override
    public void cleanupRemovedEntries(boolean cleanupRemovedEntries) {
        mapB.cleanupRemovedEntries(cleanupRemovedEntries);
    }

    @Override
    public void removedEntryCleanupTimeout(long removedEntryCleanupTimeout, TimeUnit unit) {
        mapB.removedEntryCleanupTimeout(removedEntryCleanupTimeout, unit);
    }

    @Override
    public void remoteOperations(SetRemoteOperations<K, ?> remoteOperations) {
        mapB.remoteOperations(new MapRemoteOperations<K, DummyValue, Object>() {
            @Override
            public void remove(MapRemoteQueryContext<K, DummyValue, Object> q) {
                //noinspection unchecked
                remoteOperations.remove((SetRemoteQueryContext) q);
            }

            @Override
            public void put(
                    MapRemoteQueryContext<K, DummyValue, Object> q, Data<DummyValue> newValue) {
                //noinspection unchecked
                remoteOperations.put((SetRemoteQueryContext) q);
            }
        });
    }

    @Override
    public Runnable getPreShutdownAction() {
        return mapB.getPreShutdownAction();
    }

    @Override
    public boolean skipCloseOnExitHook() {
        return mapB.skipCloseOnExitHook();
    }

    @Override
    public boolean sparseFile() {
        return mapB.sparseFile();
    }
}

