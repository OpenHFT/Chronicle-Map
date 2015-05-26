package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.HashRemoteQueryContext;
import net.openhft.chronicle.set.SetQueryContext;
import org.jetbrains.annotations.Nullable;

public interface SetRemoteQueryContext<K, R>
        extends SetQueryContext<K, R>, HashRemoteQueryContext<K> {
    @Override
    @Nullable
    SetReplicableEntry<K> entry();
}
