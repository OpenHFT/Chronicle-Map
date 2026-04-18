/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEntryOperations;
import net.openhft.chronicle.map.MapQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleMap} queries and internal replication operations.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 * @see MapRemoteOperations
 */
public interface MapRemoteQueryContext<K, V, R> extends MapQueryContext<K, V, R>,
        RemoteOperationContext<K> {
    @Nullable
    @Override
    MapReplicableEntry<K, V> entry();

    /**
     * The value used as a tombstone, for removed entries, to save space. This value has {@linkplain
     * SizeMarshaller#minStorableSize()} minimum possible size} for {@linkplain
     * ChronicleMapBuilder#valueSizeMarshaller(SizeMarshaller) the configured value size
     * marshaller}.
     * <p>
     * The returned value doesn't have object form (i.e. it's {@link Data#get()} and {@link
     * Data#getUsing(Object)} methods throw {@code UnsupportedOperationException}), it has only
     * bytes form, of all zero bytes.
     *
     * @return dummy value of all zeros, of the minimum possible size.
     */
    Data<V> dummyZeroValue();
}
