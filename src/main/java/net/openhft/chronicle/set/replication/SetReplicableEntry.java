package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.HashReplicableEntry;
import net.openhft.chronicle.set.SetEntry;

public interface SetReplicableEntry<K> extends SetEntry<K>, HashReplicableEntry<K> {
}
