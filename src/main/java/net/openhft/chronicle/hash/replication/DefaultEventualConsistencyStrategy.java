/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteOperations;

import static net.openhft.chronicle.hash.replication.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.AcceptanceDecision.DISCARD;

/**
 * Specifies the default eventual consistency strategy for {@link ChronicleHashBuilder#replication(
 * byte) replicated} {@link ChronicleHash}es:
 * <i>last write wins</i>. If two writes to a single entry occurred simultaneously on different
 * nodes, the write on the node with lower identifier wins.
 * 
 * @see MapRemoteOperations
 * @see SetRemoteOperations 
 */
public final class DefaultEventualConsistencyStrategy {

    /**
     * Returns the acceptance decision, should be made about the modification operation in the
     * given {@code context}, aiming to modify the given {@code entry}. This method doesn't do any
     * changes to {@code entry} nor {@code context} state. {@link MapRemoteOperations} and
     * {@link SetRemoteOperations} method implementations should guide the result of calling this
     * method to do something to <i>actually</i> apply the remote operation.
     *  
     * @param entry the entry to be modified
     * @param context the remote operation context
     * @return if the remote operation should be accepted or discarded
     */
    public static AcceptanceDecision decideOnRemoteModification(
            ReplicableEntry entry, RemoteOperationContext<?> context) {
        long remoteTimestamp = context.remoteTimestamp();
        long originTimestamp = entry.originTimestamp();
        boolean shouldAccept = remoteTimestamp > originTimestamp ||
                (remoteTimestamp == originTimestamp &&
                        context.remoteIdentifier() <= entry.originIdentifier());
        return shouldAccept ? ACCEPT : DISCARD;
    }
    
    private DefaultEventualConsistencyStrategy() {}
}
