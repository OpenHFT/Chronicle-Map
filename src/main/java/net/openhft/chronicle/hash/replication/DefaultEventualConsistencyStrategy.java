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
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteOperations;

import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.DISCARD;

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
        // Last write wins
        if (remoteTimestamp > originTimestamp)
            return ACCEPT;
        if (remoteTimestamp < originTimestamp)
            return DISCARD;
        // remoteTimestamp == originTimestamp below
        byte remoteIdentifier = context.remoteIdentifier();
        byte originIdentifier = entry.originIdentifier();
        // Lower identifier wins
        if (remoteIdentifier < originIdentifier)
            return ACCEPT;
        if (remoteIdentifier > originIdentifier)
            return DISCARD;
        // remoteTimestamp == originTimestamp && remoteIdentifier == originIdentifier below
        // This could be, only if a node with the origin identifier was lost, a new Chronicle Hash
        // instance was started up, but with timeProvider which for some reason is very late, so
        // that it provides the same time, as the "old" node with this identifier, before it was
        // lost. (This is almost a theoretical situation.) In this case, give advantage to fresh
        // entry updates to the "new" node. Entries with the same id and timestamp, bootstrapped
        // "back" from other nodes in the system, are discarded on this new node (this is the
        // of the condition originIdentifier == thisReplica.identifier()). But those new updates
        // should win on other nodes.
        return originIdentifier == ((Replica) context.hash()).identifier() ? DISCARD : ACCEPT;
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
