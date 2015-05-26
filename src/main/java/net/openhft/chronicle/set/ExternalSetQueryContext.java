package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ExternalHashQueryContext;

public interface ExternalSetQueryContext<K, R>
        extends SetQueryContext<K, R>, ExternalHashQueryContext<K> {
}
