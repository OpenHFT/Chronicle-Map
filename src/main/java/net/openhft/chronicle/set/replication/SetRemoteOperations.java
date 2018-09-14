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

package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.set.SetAbsentEntry;

import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

public interface SetRemoteOperations<K, R> {

    default void remove(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                ReplicableEntry replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                assert replicableAbsentEntry != null;
                replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // See MapRemoteOperations
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    replicableAbsentEntry.raiseChangedForAllExcept(q.remoteNodeIdentifier());
                    replicableAbsentEntry.dropChangedFor(q.remoteNodeIdentifier());
                } else {
                    replicableAbsentEntry.dropChanged();
                }
            }
        } else {
            SetAbsentEntry<K> absentEntry = q.absentEntry();
            assert absentEntry != null;
            ReplicableEntry replicableAbsentEntry;
            if (!(absentEntry instanceof ReplicableEntry)) {
                // Note in the two following lines dummy entry is inserted and removed using direct
                // entry.doXxx calls, not q.xxx(entry). The intention is to avoid calling possibly
                // overridden SetEntryOperations, because this is technical procedure of making
                // "truly absent" entry "deleted", not actual insertion and removal.
                absentEntry.doInsert();
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
            // See MapRemoteOperations
            if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                replicableAbsentEntry.raiseChangedForAllExcept(q.remoteNodeIdentifier());
                replicableAbsentEntry.dropChangedFor(q.remoteNodeIdentifier());
            } else {
                replicableAbsentEntry.dropChanged();
            }
        }
    }

    default void put(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // See MapRemoteOperations
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    entry.raiseChanged();
                } else {
                    entry.dropChanged();
                }
            }
        } else {
            SetAbsentEntry<K> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (!(absentEntry instanceof ReplicableEntry) ||
                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) == ACCEPT) {
                q.insert(absentEntry);
                entry = q.entry(); // q.entry() is not null after insert
                assert entry != null;
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                // See MapRemoteOperations
                if (q.remoteIdentifier() == q.currentNodeIdentifier()) {
                    entry.raiseChanged();
                } else {
                    entry.dropChanged();
                }
            } else {
                // See MapRemoteOperations
                if (((ReplicableEntry) absentEntry).originIdentifier() == q.remoteIdentifier() &&
                        q.remoteIdentifier() != q.currentNodeIdentifier()) {
                    ((ReplicableEntry) absentEntry).raiseChangedFor(q.remoteIdentifier());
                }
            }
        }
    }
}
