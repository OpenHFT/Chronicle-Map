/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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
