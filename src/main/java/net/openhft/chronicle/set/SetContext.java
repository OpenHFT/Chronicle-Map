package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashContext;

/**
 * Context, in which {@link SetEntry SetEntries} are accessed.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
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
