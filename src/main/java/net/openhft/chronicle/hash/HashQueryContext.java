package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;

public interface HashQueryContext<K> extends HashContext<K>, InterProcessReadWriteUpdateLock {

    /**
     * Returns the queried key as a {@code Value}.
     */
    Value<K, ?> queriedKey();

    /**
     * Returns the entry access object, if the entry with the queried key is <i>present</i>
     * in the hash, returns {@code null} is the entry is <i>absent</i>. Might acquire
     * {@link #readLock} before searching for the key, if the context is not locked yet.
     */
    HashEntry<K> entry();

    /**
     * Returns the special <i>absent entry</i> object, if the entry with the queried key
     * is <i>absent</i> in the hash, returns {@code null}, if the entry is <i>present</i>. Might
     * acquire {@link #readLock} before searching for the key, if the context is not locked yet.
     */
    HashAbsentEntry<K> absentEntry();
}
