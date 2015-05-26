package net.openhft.chronicle.set;

import org.jetbrains.annotations.NotNull;

public interface SetEntryOperations<K, R> {
    
    default R remove(@NotNull SetEntry<K> entry) {
        entry.doRemove();
        return null;
    }
    
    default R insert(@NotNull SetAbsentEntry<K> entry) {
        entry.doInsert();
        return null;
    }
}
