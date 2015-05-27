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
     * Could throw only unchecked exception. 
     */
    @Override
    void close();
}
