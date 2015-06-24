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

package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntryOperations;

import static net.openhft.chronicle.hash.replication.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

/**
 * SPI strategy of performing remote calls and apply replication events for {@link ChronicleMap}.
 *
 * <p>Example: Grow-only set values CRDT: <pre><code>
 * class GrowOnlySetValuedMapEntryOperations&lt;K, E&gt;
 *         implements MapEntryOperations&lt;K, Set&lt;E&gt;, Void&gt; {
 *     &#064;Override
 *     public Void remove(@NotNull MapEntry&lt;K, Set&lt;E&gt;&gt; entry) {
 *         throw new UnsupportedOperationException("Map with grow-only set values " +
 *                 "doesn't support map value removals");
 *     }
 * }
 *
 * class GrowOnlySetValuedMapRemoteOperations&lt;K, E&gt;
 *         implements MapRemoteOperations&lt;K, Set&lt;E&gt;, Void&gt; {
 *     &#064;Override
 *     public void put(MapRemoteQueryContext&lt;K, Set&lt;E&gt;, Void&gt; q,
 *                     Data&lt;Set&lt;E&gt;, ?&gt; newValue) {
 *         MapReplicableEntry&lt;K, Set&lt;E&gt;&gt; entry = q.entry();
 *         if (entry != null) {
 *             Set&lt;E&gt; merged = new HashSet&lt;&gt;(entry.value().get());
 *             merged.addAll(newValue.get());
 *             q.replaceValue(entry, q.wrapValueAsValue(merged));
 *         } else {
 *             q.insert(q.absentEntry(), newValue);
 *             q.entry().updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
 *         }
 *     }
 *
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
 * @see ChronicleMapBuilder#remoteOperations(MapRemoteOperations)
 */
public interface MapRemoteOperations<K, V, R> {

    /**
     * Handle remote {@code remove} call and {@code remove} replication event, i. e. when the entry
     * with the query key ({@code q.queriedKey()}) was removed on some {@code ChronicleMap} node.
     *
     * @implNote the default implementation applies the remove event using {@link
     * DefaultEventualConsistencyStrategy} "latest wins" strategy: <pre>{@code
     * MapReplicableEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     if (DefaultEventualConsistencyStrategy.decideOnRemoteModification(entry, q) == ACCEPT) {
     *         q.remove(entry);
     *         ReplicableEntry replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
     *         replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
     *         replicableAbsentEntry.dropChanged();
     *     }
     * } else {
     *     MapAbsentEntry<K, V> absentEntry = q.absentEntry();
     *     ReplicableEntry replicableAbsentEntry;
     *     if (!(absentEntry instanceof ReplicableEntry)) {
     *         // Note in the two following lines dummy value is inserted and removed using direct
     *         // entry.doXxx calls, not q.xxx(entry). The intention is to avoid calling possibly
     *         // overridden MapEntryOperations, because this is technical procedure of making
     *         // "truly absent" entry "deleted", not actual insertion and removal.
     *         absentEntry.doInsert(q.dummyZeroValue());
     *         q.entry().doRemove();
     *         replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
     *     } else {
     *         replicableAbsentEntry = (ReplicableEntry) absentEntry;
     *         if (DefaultEventualConsistencyStrategy.decideOnRemoteModification(
     *                 replicableAbsentEntry, q) == DISCARD) {
     *             return;
     *         }
     *     }
     *     replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
     *     replicableAbsentEntry.dropChanged();
     * }}</pre>
     *
     * @param q the remote operation context
     */
    default void remove(MapRemoteQueryContext<K, V, R> q) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                ReplicableEntry replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                replicableAbsentEntry.dropChanged();
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            ReplicableEntry replicableAbsentEntry;
            if (!(absentEntry instanceof ReplicableEntry)) {
                // Note in the two following lines dummy value is inserted and removed using direct
                // entry.doXxx calls, not q.xxx(entry). The intention is to avoid calling possibly
                // overridden MapEntryOperations, because this is technical procedure of making
                // "truly absent" entry "deleted", not actual insertion and removal.
                absentEntry.doInsert(q.dummyZeroValue());
                q.entry().doRemove();
                replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
            } else {
                replicableAbsentEntry = (ReplicableEntry) absentEntry;
                if (decideOnRemoteModification(replicableAbsentEntry, q) == DISCARD)
                    return;
            }
            replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
            replicableAbsentEntry.dropChanged();
        }
    }

    /**
     * Handle remote {@code put} call or replication event, i. e. when the entry with the queried
     * key ({@code q.queriedKey()}) was changed on some remote {@code ChronicleMap} node, with the
     * given {@code newValue}.
     *
     * @implNote the default implementation applies the put event using {@link
     * DefaultEventualConsistencyStrategy} "latest wins" strategy: <pre>{@code
     * MapReplicableEntry<K, V> entry = q.entry();
     * if (entry != null) {
     *     if (DefaultEventualConsistencyStrategy.decideOnRemoteModification(entry, q) == ACCEPT) {
     *         q.replaceValue(entry, newValue);
     *         entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
     *         entry.dropChanged();
     *     }
     * } else {
     *     MapAbsentEntry<K, V> absentEntry = q.absentEntry();
     *     if (!(absentEntry instanceof ReplicableEntry) ||
     *             DefaultEventualConsistencyStrategy.decideOnRemoteModification(
     *                     (ReplicableEntry) absentEntry, q) == ACCEPT) {
     *         q.insert(absentEntry, newValue);
     *         entry = q.entry();
     *         entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
     *         entry.dropChanged();
     *     }
     * }}</pre>
     *
     * @param q the remote operation context
     * @param newValue the new value to put
     */
    default void put(MapRemoteQueryContext<K, V, R> q, Data<V> newValue) {
        MapReplicableEntry<K, V> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.replaceValue(entry, newValue);
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                entry.dropChanged();
            }
        } else {
            MapAbsentEntry<K, V> absentEntry = q.absentEntry();
            if (!(absentEntry instanceof ReplicableEntry) ||
                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) == ACCEPT) {
                q.insert(absentEntry, newValue);
                entry = q.entry();
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                entry.dropChanged();
            }
        }
    }
}
