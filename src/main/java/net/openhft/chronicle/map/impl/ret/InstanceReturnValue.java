package net.openhft.chronicle.map.impl.ret;

import net.openhft.chronicle.map.ReturnValue;

public interface InstanceReturnValue<V> extends ReturnValue<V> {
    V returnValue();
}
