package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableData;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.data.instance.ValueInitializableData;

public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {
    
    void initInputKey(Data<K> inputKey);
    
    KeyInitableData<K> inputKeyInstanceValue();
    
    InstanceReturnValue<V> defaultReturnValue();
    
    UsableReturnValue<V> usingReturnValue();
    
    ValueInitializableData<V> inputValueInstanceValue();
    
    MapKeyContext<K, V> deprecatedMapKeyContext();

    Closeable acquireHandle();
    
    void initTheSegmentIndex(int segmentIndex);
    
    boolean theSegmentIndexInit();
    
    void clear();
}
