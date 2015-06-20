package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashContext;

/**
 * Context, in which {@link SetEntry SetEntries} are accessed. {@code SetContext} allows to access
 * {@link SetEntryOperations}, configured for the accessed {@link ChronicleSet}. {@code SetContext}
 * implements {@code SetEntryOperations} by delegation to the configured {@code entryOperations}.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specified for the queried set
 */
public interface SetContext<K, R> extends HashContext<K>, SetEntryOperations<K, R> {

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #set()}.
     */
    @Override
    default ChronicleSet<K> hash() {
        return set();
    }

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #hash()}.
     */
    ChronicleSet<K> set();
}
