package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.HashContext;

public interface SetContext<K, R> extends HashContext<K>, SetEntryOperations<K, R> {

    @Override
    default ChronicleHash<K, ?, ?> hash() {
        return set();
    }

    ChronicleSet<K> set();
}
