package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import org.jetbrains.annotations.NotNull;

public final class NullReturnValue implements InstanceReturnValue {
    private static final NullReturnValue NULL_RETURN_VALUE = new NullReturnValue();
    
    public static <V> InstanceReturnValue<V> get() {
        return NULL_RETURN_VALUE;
    }
    
    private NullReturnValue() {}

    @Override
    public Object returnValue() {
        return null;
    }

    @Override
    public void returnValue(@NotNull Data value) {
        // ignore
    }
}
