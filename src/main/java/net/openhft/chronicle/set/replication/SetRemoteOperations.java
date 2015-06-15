package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.AcceptanceDecision;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.set.SetAbsentEntry;

import static net.openhft.chronicle.hash.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;

public interface SetRemoteOperations<K, R> {
    
    default AcceptanceDecision remove(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                q.remove(entry);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        } else {
            SetAbsentEntry<K> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (absentEntry instanceof ReplicableEntry) {
                return decideOnRemoteModification((ReplicableEntry) absentEntry, q);
            } else {
                return ACCEPT;
            }
        }
    }

    default AcceptanceDecision insert(SetRemoteQueryContext<K, R> q) {
        SetReplicableEntry<K> entry = q.entry();
        if (entry != null) {
            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                return ACCEPT;
            } else {
                return DISCARD;
            }
        } else {
            SetAbsentEntry<K> absentEntry = q.absentEntry();
            assert absentEntry != null;
            if (!(absentEntry instanceof ReplicableEntry) ||
                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) == ACCEPT) {
                q.insert(absentEntry);
                return ACCEPT;
            } else {
                return DISCARD;
            }
        }
    }
}
