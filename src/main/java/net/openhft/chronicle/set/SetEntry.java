package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashEntry;

/**
 * The <i>present</i> {@link ChronicleSet} entry context.
 *  
 * @param <K> the set key type
 * @see SetEntryOperations
 * @see SetQueryContext#entry() 
 */
public interface SetEntry<K> extends HashEntry<K> {
    @Override
    SetContext<K, ?> context();

    /**
     * Removes the entry from the {@code ChronicleSet}.
     * 
     * <p>This method is the default implementation for {@link SetEntryOperations#remove(SetEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
