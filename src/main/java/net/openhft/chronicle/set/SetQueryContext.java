package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashQueryContext;
import org.jetbrains.annotations.Nullable;

public interface SetQueryContext<K, R> extends HashQueryContext<K>, SetContext<K, R> {

    @Override
    @Nullable
    SetEntry<K> entry();

    @Override
    @Nullable
    SetAbsentEntry<K> absentEntry();
}
