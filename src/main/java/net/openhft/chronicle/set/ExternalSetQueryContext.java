package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ExternalHashQueryContext;

/**
 * {@link SetQueryContext} + {@link AutoCloseable}, for external {@link ChronicleSet} queries
 * in <i>try-with-resources</i> blocks.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations}, specified for the queried {@code
 * ChronicleSet}
 * @see ChronicleSet#queryContext(Object)
 */
public interface ExternalSetQueryContext<K, R>
        extends SetQueryContext<K, R>, ExternalHashQueryContext<K> {
}
