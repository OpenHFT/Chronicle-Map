package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class WrappedValueInstanceValueHolder<K, V, R> implements MapContext<K, V, R> {

    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef WrappedValueInstanceData<V, ?, ?> wrappedValueInstanceValue;


    @Override
    public Data<V> wrapValueAsData(V value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        WrappedValueInstanceData<V, ?, ?> wrapped = this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValue();
        wrapped.initValue(value);
        return wrapped;
    }
}
