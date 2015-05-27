package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashAbsentEntry;

/**
 * Low-level operational context for the situations, when the new key is going to be inserted
 * into the {@link ChronicleSet}.
 *
 * @param <K> the set key type
 *
 * @see SetEntryOperations
 * @see SetQueryContext#absentEntry()
 */
public interface SetAbsentEntry<K> extends HashAbsentEntry<K> {
    @Override
    SetContext<K, ?> context();

    /**
     * Inserts {@link #absentKey() the new key} into the set.
     *
     * <p>This method is the default implementation for {@link SetEntryOperations#insert(
     * SetAbsentEntry)}, which might be customized over the default.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     * operation are not met
     * @see SetEntryOperations#insert(SetAbsentEntry)
     */
    void doInsert();
}
