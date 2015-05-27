package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.HashRemoteQueryContext;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetEntryOperations;
import net.openhft.chronicle.set.SetQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleSet} queries and internal replication operations.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see SetRemoteOperations
 */
public interface SetRemoteQueryContext<K, R>
        extends SetQueryContext<K, R>, HashRemoteQueryContext<K> {
    @Override
    @Nullable
    SetReplicableEntry<K> entry();
}
