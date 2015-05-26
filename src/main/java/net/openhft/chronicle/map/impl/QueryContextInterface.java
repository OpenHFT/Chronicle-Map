package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Value;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableValue;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.value.instance.ValueInitableValue;

public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {
    
    void initInputKey(Value<K, ?> inputKey);
    
    KeyInitableValue<K, ?> inputKeyInstanceValue();
    
    InstanceReturnValue<V> defaultReturnValue();
    
    UsableReturnValue<V> usingReturnValue();
    
    ValueInitableValue<V, ?> inputValueInstanceValue();
    
    MapKeyContext<K, V> deprecatedMapKeyContext();
    
    MapKeyContext<K, V> deprecatedMapAcquireContext();
    
    void initTheSegmentIndex(int segmentIndex);
    
    boolean theSegmentIndexInit();
    
    void initEntry(long pos);
    
    void clear();
}
