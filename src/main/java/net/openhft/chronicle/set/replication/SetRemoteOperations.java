/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
