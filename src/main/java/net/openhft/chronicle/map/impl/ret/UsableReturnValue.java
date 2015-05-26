package net.openhft.chronicle.map.impl.ret;

public interface UsableReturnValue<V> extends InstanceReturnValue<V> {
     Object USING_RETURN_VALUE_UNINT = new Object();
    
    void initUsingReturnValue(V usingValue);
}
