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
