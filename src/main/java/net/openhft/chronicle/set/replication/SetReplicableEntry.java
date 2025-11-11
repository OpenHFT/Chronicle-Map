/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetEntry;

/**
 * A context of a <i>present</i> entry in the replicated {@link ChronicleSet}.
 *
 * @param <K> the set key type
 * @see SetRemoteOperations
 * @see SetRemoteQueryContext
 */
public interface SetReplicableEntry<K> extends SetEntry<K>, ReplicableEntry {
}
