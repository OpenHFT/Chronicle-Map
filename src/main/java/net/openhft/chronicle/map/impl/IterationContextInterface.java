package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapSegmentContext;

public interface IterationContextInterface<K, V, R> extends MapEntry<K, V>,
        MapSegmentContext<K, V, R> {
    long pos();
    
    void initTheSegmentIndex(int segmentIndex);
}
