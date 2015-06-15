package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

import java.util.function.Consumer;

public interface ReplicatedIterationContextInterface<K, V> extends IterationContextInterface<K, V> {

    void forEachReplicableEntry(Consumer<? super ReplicableEntry> action);

    void readExistingEntry(long pos);
}
