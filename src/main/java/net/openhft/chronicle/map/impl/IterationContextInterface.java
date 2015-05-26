package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapKeyContext;

import java.util.function.Predicate;

public interface IterationContextInterface<K, V> extends MapEntry<K, V>, AutoCloseable {
    long pos();
    
    void initTheSegmentIndex(int segmentIndex);
    
    MapKeyContext<K, V> deprecatedMapKeyContextOnIteration();
    
    boolean forEachRemoving(Predicate<? super MapEntry<K, V>> action);

    @Override
    void close();
    
    long size();
}
