package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class ReplicatedMapAbsent<K, V> extends MapAbsent<K, V> {

    @StageRef MapQuery<K, V, ?> q;
    @StageRef ReplicatedMapEntryStages<K, V, ?> e;
    @StageRef ReplicationUpdate<K> ru;

    @Override
    public void doInsert(Data<V> value) {
        q.putPrefix();
        if (!q.entryPresent()) {
            if (!q.searchStatePresent()) {
                putEntry(value);
                q.setSearchState(HashQuery.SearchState.PRESENT);
            } else {
                e.innerDefaultReplaceValue(value);
                s.deleted(s.deleted() - 1);
            }
            s.incrementModCount();
            e.writeEntryPresent();
            ru.updateChange();
            e.updatedReplicationStateOnAbsentEntry();
        } else {
            throw new IllegalStateException("Entry is absent in the map when doInsert() is called");
        }
    }
}
