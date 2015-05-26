package net.openhft.chronicle.hash;

import org.jetbrains.annotations.NotNull;

public interface HashAbsentEntry<K> {
    
    /**
     * Returns the context, in which the entry is going to be inserted into the hash.
     */
    HashContext<K> context();
    
    /**
     * Returns the key is going to be inserted into the {@code ChronicleHash}.
     */
    @NotNull
    Value<K, ?> absentKey();
}
