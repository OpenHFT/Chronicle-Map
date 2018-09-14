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

package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntryOperations;

import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

/**
 * SPI strategy of performing remote calls and apply replication events for {@link ChronicleMap}.
 * <p>
 * <p>Example: Grow-only set values CRDT: <pre><code>
 * class GrowOnlySetValuedMapEntryOperations&lt;K, E&gt;
 *         implements MapEntryOperations&lt;K, Set&lt;E&gt;, Void&gt; {
 *     &#064;Override
 *     public Void remove(@NotNull MapEntry&lt;K, Set&lt;E&gt;&gt; entry) {
 *         throw new UnsupportedOperationException("Map with grow-only set values " +
 *                 "doesn't support map value removals");
 *     }
 * }
 * <p>
 * class GrowOnlySetValuedMapRemoteOperations&lt;K, E&gt;
 *         implements MapRemoteOperations&lt;K, Set&lt;E&gt;, Void&gt; {
 *     &#064;Override
 *     public void put(MapRemoteQueryContext&lt;K, Set&lt;E&gt;, Void&gt; q,
 *                     Data&lt;Set&lt;E&gt;, ?&gt; newValue) {
 *         MapReplicableEntry&lt;K, Set&lt;E&gt;&gt; entry = q.entry();
 *         if (entry != null) {
 *             Set&lt;E&gt; merged = new HashSet&lt;&gt;(entry.value().get());
 *             merged.addAll(newValue.get());
 *             q.replaceValue(entry, q.wrapValueAsData(merged));
 *         } else {
 *             q.insert(q.absentEntry(), newValue);
 *             q.entry().updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
 *         }
 *     }
 * <p>
 *     &#064;Override
 *     public void remove(MapRemoteQueryContext&lt;K, Set&lt;E&gt;, Void&gt; q) {
 *         throw new UnsupportedOperationException();
 *     }
 * }</code></pre>
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specified fro the queried map
 * @see DefaultEventualConsistencyStrategy
 * @see net.openhft.chronicle.map.ChronicleMapBuilderPrivateAPI#remoteOperations(MapRemoteOperations)
 */
public interface MapRemoteOperations<K, V, R> {

    /**
     * Handle remote {@code remove} call and {@code remove} replication event, i. e. when the entry
     * with the query key ({@code q.queriedKey()}) was removed on some {@code ChronicleMap} node.
     *
     * @param q the remote operation context
     */
    default void remove(MapRemoteQueryContext<K, V, R> q) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                ReplicableEntry replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                assert replicableAbsentEntry != null;
                replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // (*)
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    // The entry with origin (replicatedIdentifier() is remote entry's origin),
                    // equal to the current node, should be lost on on this node (or this node is
                    // a different Chronicle Map instance, because the previous was lost, just using
                    // the same identifier), if it happens we should propagate this recovered
                    // event again, to other nodes
                    replicableAbsentEntry.raiseChangedForAllExcept(q.remoteNodeIdentifier());
                    replicableAbsentEntry.dropChangedFor(q.remoteNodeIdentifier());
                } else {
                    // We accepted a replication event, suppress further event propagation
                    replicableAbsentEntry.dropChanged();
                }
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            assert absentEntry != null;
            ReplicableEntry replicableAbsentEntry;
            if (!(absentEntry instanceof ReplicableEntry)) {
                // Note in the two following lines dummy value is inserted and removed using direct
                // entry.doXxx calls, not q.xxx(entry). The intention is to avoid calling possibly
                // overridden MapEntryOperations, because this is technical procedure of making
                // "truly absent" entry "deleted", not actual insertion and removal.
                absentEntry.doInsert(q.dummyZeroValue());
                entry = q.entry();
                assert entry != null;
                entry.doRemove();
                replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                assert replicableAbsentEntry != null;
            } else {
                replicableAbsentEntry = (ReplicableEntry) absentEntry;
                if (decideOnRemoteModification(replicableAbsentEntry, q) == DISCARD)
                    return;
            }
            replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
            // For explanation see similar block above (*)
            if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                replicableAbsentEntry.raiseChangedForAllExcept(q.remoteNodeIdentifier());
                replicableAbsentEntry.dropChangedFor(q.remoteNodeIdentifier());
            } else {
                replicableAbsentEntry.dropChanged();
            }
        }
    }

    /**
     * Handle remote {@code put} call or replication event, i. e. when the entry with the queried
     * key ({@code q.queriedKey()}) was changed on some remote {@code ChronicleMap} node, with the
     * given {@code newValue}.
     *
     * @param q        the remote operation context
     * @param newValue the new value to put
     */
    default void put(MapRemoteQueryContext<K, V, R> q, Data<V> newValue) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.replaceValue(entry, newValue);
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // For explanation see similar block in remove() method (*)
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    // This differs from action under the same conditions in remove() method,
                    // to let the item 10 in (**) work!
                    entry.raiseChanged();
                } else {
                    entry.dropChanged();
                }
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (!(absentEntry instanceof ReplicableEntry) ||
                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) == ACCEPT) {
                q.insert(absentEntry, newValue);
                entry = q.entry();
                assert entry != null;
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // For explanation see similar block in remove() method (*)
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    // This differs from action under the same conditions in remove() method,
                    // to let the item 10 in (**) work!
                    entry.raiseChanged();
                } else {
                    entry.dropChanged();
                }
            } else {
                // (**)
                // In case of old deleted entries cleanup + network disconnections and
                // bootstrapping, it is possible that the entry ends being removed on the remote
                // node and present on the origin node:
                //
                // 1. Entry is added on node 1
                // 2. This change is replicated and arrived to node 2
                // 3. Entry is removed on node 1, it is scheduled for replication again
                // 4. Network issues, disconnection, bootstrapping
                // 5. Node 2 schedules the subject entry (currently present on node 2)
                //    for bootstrapping to the node 1
                // 6. Node 2 sends the replication event to node 1 (in-flight)
                // 7. The entry removal is replicated from node 1 to node 2, it is accepted, finally
                //    the entry is removed on node 2
                // 8. The entry is cleaned up (erased completely) on node 1, because it is already
                //    replicated to everywhere it should be, and timeout (if set very short)
                //    is expired
                // 9. Replication event from node 2 (with present entry) arrives to node 1, as the
                //    entry is already erased from node 1, this change is accepted, and finally
                //    the entry is present on node 1.
                // 10. The entry is bootstrapped again on node 1, by (*) blocks
                // 11. The "present" entry from node 1 is discarded on node 2, because it sees that
                //     this entry was removed later.
                //
                // The following block captures the condition from item 11, and sends the event
                // again back, to give the chance to be finally removed on node 1 (in the above
                // example).
                if (((ReplicableEntry) absentEntry).originIdentifier() == q.remoteIdentifier() &&
                        q.remoteIdentifier() != q.currentNodeIdentifier()) {
                    // If this change will arrive to the origin node and accepted, it will be
                    // propagated to all other nodes, by (*) blocks
                    ((ReplicableEntry) absentEntry).raiseChangedFor(q.remoteIdentifier());
                }
            }
        }
    }
}
