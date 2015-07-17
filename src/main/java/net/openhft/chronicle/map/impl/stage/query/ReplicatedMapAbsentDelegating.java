package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapAbsentDelegating<K, V> implements MapAbsentEntry<K, V> {

    @StageRef ReplicatedMapAbsent<K, V> delegate;

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return delegate.context();
    }

    @Override
    public void doInsert(Data<V> value) {
        delegate.doInsert(value);
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        return delegate.defaultValue();
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        return delegate.absentKey();
    }
}
