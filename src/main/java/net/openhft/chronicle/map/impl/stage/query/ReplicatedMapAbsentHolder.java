package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.MapAbsentEntryHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class ReplicatedMapAbsentHolder<K, V> implements MapAbsentEntryHolder<K, V> {

    @StageRef ReplicatedMapAbsent<K, V> ab;

    @Override
    public MapAbsentEntry<K, V> absent() {
        return ab;
    }
}
