package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.set.SetAbsentEntry;

import static net.openhft.chronicle.hash.replication.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

public interface SetRemoteOperations<K, R> {
    
    default void remove(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                ReplicableEntry replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                replicableAbsentEntry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                replicableAbsentEntry.dropChanged();
            }
        } else {
            SetAbsentEntry<K> absentEntry = q.absentEntry();
            ReplicableEntry replicableAbsentEntry;
            if (!(absentEntry instanceof ReplicableEntry)) {
                // Note in the two following lines dummy entry is inserted and removed using direct
                // entry.doXxx calls, not q.xxx(entry). The intention is to avoid calling possibly
                // overridden SetEntryOperations, because this is technical procedure of making
                // "truly absent" entry "deleted", not actual insertion and removal.
                absentEntry.doInsert();
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

    default void insert(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                entry.dropChanged();
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
                entry.dropChanged();
            }
        }
    }
}
