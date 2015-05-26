package net.openhft.chronicle.map;

import net.openhft.chronicle.map.replication.MapRemoteOperations;

final class DefaultSpi implements MapMethods, MapEntryOperations, MapRemoteOperations {
    static final DefaultSpi DEFAULT_SPI = new DefaultSpi();
    static <K, V, R> MapMethods<K, V, R> mapMethods() {
        return DEFAULT_SPI;
    }
    
    static <K, V, R> MapEntryOperations<K, V, R> mapEntryOperations() {
        return DEFAULT_SPI;
    }
    
    static <K, V, R> MapRemoteOperations<K, V, R> mapRemoteOperations() {
        return DEFAULT_SPI;
    }
}
