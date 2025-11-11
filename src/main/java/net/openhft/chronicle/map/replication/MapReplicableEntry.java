/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntry;

/**
 * A context of a <i>present</i> entry in the replicated {@link ChronicleMap}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see MapRemoteOperations
 * @see MapRemoteQueryContext
 */
public interface MapReplicableEntry<K, V> extends MapEntry<K, V>, ReplicableEntry {
}
