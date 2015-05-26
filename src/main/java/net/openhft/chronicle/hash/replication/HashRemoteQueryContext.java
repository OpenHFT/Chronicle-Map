package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.HashQueryContext;

public interface HashRemoteQueryContext<K> extends HashQueryContext<K>, RemoteOperationContext<K> {
    @Override
    HashReplicableEntry<K> entry();
}
