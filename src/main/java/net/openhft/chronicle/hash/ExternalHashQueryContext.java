package net.openhft.chronicle.hash;

/**
 * {@link HashQueryContext} + {@link AutoCloseable}, for external {@link ChronicleHash} queries
 * in <i>try-with-resources</i> blocks. 
 * 
 * @param <K> the hash key type
 * @see ChronicleHash#queryContext(Object)
 */
public interface ExternalHashQueryContext<K> extends HashQueryContext<K>, AutoCloseable {

    /**
     * Closes the query context, automatically releases all locks and disposes all resources,
     * acquired during the query operation. I. e. you shouldn't release locks manually in the end
     * of try-with-resources statement: <pre>{@code
     * try (ExternalHashQueryContext<K> q = hash.queryContext(key))
     *     q.writeLock().lock();
     *     // ...make a query under exclusive lock
     *     // NOT NEEDED - q.readLock().unlock();
     * }}</pre>
     */
    @Override
    void close();
}
