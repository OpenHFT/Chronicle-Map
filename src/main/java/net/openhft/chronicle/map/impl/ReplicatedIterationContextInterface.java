package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

import java.util.function.Consumer;

public interface ReplicatedIterationContextInterface<K, V, R>
        extends IterationContextInterface<K, V, R> {

    void forEachReplicableEntry(Consumer<? super ReplicableEntry> action);

    void readExistingEntry(long pos);
}
