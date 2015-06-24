package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.set.SetAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleHash}.
 * 
 * <p>This interface is not usable by itself; it merely defines the common base for {@link
 * MapAbsentEntry} and {@link SetAbsentEntry}.
 *  
 * @param <K> the hash key type 
 * @see HashQueryContext#absentEntry()
 */
public interface HashAbsentEntry<K> {
    
    /**
     * Returns the context, in which the entry is going to be inserted into the hash.
     */
    HashContext<K> context();
    
    /**
     * Returns the key is going to be inserted into the {@code ChronicleHash}.
     */
    @NotNull
    Data<K> absentKey();
}
