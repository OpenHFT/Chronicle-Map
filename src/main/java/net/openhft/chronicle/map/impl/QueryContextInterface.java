package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableData;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.data.instance.ValueInitableData;

public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {
    
    void initInputKey(Data<K> inputKey);
    
    KeyInitableData<K> inputKeyInstanceValue();
    
    InstanceReturnValue<V> defaultReturnValue();
    
    UsableReturnValue<V> usingReturnValue();
    
    ValueInitableData<V> inputValueInstanceValue();
    
    MapKeyContext<K, V> deprecatedMapKeyContext();
    
    MapKeyContext<K, V> deprecatedMapAcquireContext();
    
    void initTheSegmentIndex(int segmentIndex);
    
    boolean theSegmentIndexInit();
    
    void clear();
}
