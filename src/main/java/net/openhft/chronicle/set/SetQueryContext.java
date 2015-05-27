package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashQueryContext;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntryOperations;
import net.openhft.chronicle.map.MapMethods;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteOperations;
import org.jetbrains.annotations.Nullable;

/**
 * The context of {@link ChronicleSet} operations with <i>individual keys</i>
 * (most: {@code contains()}, {@code add()}, etc., opposed to <i>bulk</i> operations).
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see ChronicleSet#queryContext(Object)
 */
public interface SetQueryContext<K, R> extends HashQueryContext<K>, SetContext<K, R> {

    @Override
    @Nullable
    SetEntry<K> entry();

    @Override
    @Nullable
    SetAbsentEntry<K> absentEntry();
}
