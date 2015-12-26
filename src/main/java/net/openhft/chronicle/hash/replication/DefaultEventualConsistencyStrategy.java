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
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteOperations;

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
                        context.remoteIdentifier() < entry.originIdentifier());
        return shouldAccept ? AcceptanceDecision.ACCEPT : AcceptanceDecision.DISCARD;
    }
    
    private DefaultEventualConsistencyStrategy() {}

    /**
     * Decision, if {@link MapRemoteOperations remote modification operation} should be accepted
     * or discarded. Used in {@link DefaultEventualConsistencyStrategy}.
     */
    public enum AcceptanceDecision {
        /**
         * Acceptance decision -- the remote modification operation is applied to the local
         * {@link ChronicleHash} state.
         */
        ACCEPT,

        /**
         * Discard decision -- the remote modification operation is rejected.
         */
        DISCARD
    }
}
