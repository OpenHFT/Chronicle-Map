package net.openhft.chronicle.hash;

public interface ExternalHashQueryContext<K> extends HashQueryContext<K>, AutoCloseable {

    @Override
    void close();
}
