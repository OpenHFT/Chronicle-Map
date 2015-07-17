package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class DefaultReturnValue<V> implements InstanceReturnValue<V> {
    private V defaultReturnedValue = null;
    
    abstract boolean defaultReturnedValueInit();
    
    private void initDefaultReturnedValue(@NotNull Data<V> value) {
        defaultReturnedValue = value.getUsing(null);
    }
    
    @Override
    public void returnValue(@NotNull Data<V> value) {
        initDefaultReturnedValue(value);
    }
    
    @Override
    public V returnValue() {
        if (defaultReturnedValueInit()) {
            return defaultReturnedValue;
        } else {
            return null;
        }
    }
}
