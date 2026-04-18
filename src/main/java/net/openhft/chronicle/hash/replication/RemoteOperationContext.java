/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.HashContext;

/**
 * Context of internal replication operation.
 *
 * @param <K> the key type of accessed {@link ChronicleHash}
 */
public interface RemoteOperationContext<K> extends HashContext<K> {

    /**
     * {@link ReplicableEntry#originIdentifier()} of the replicated entry.
     */
    byte remoteIdentifier();

    /**
     * {@link ReplicableEntry#originTimestamp()} of the replicated entry.
     */
    long remoteTimestamp();

    /**
     * Returns the identifier of the current Chronicle Node (this context object is obtained on).
     */
    byte currentNodeIdentifier();

    /**
     * Returns the identifier of the node, from which current replication event came from, or -1 if
     * unknown or not applicable (the current replication event came not from another node, but,
     * e. g., applied manually).
     */
    byte remoteNodeIdentifier();
}
