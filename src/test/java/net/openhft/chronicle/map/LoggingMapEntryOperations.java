/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LoggingMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {

    static final Logger LOG = LoggerFactory.getLogger(LoggingMapEntryOperations.class);

    @Override
    public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        LOG.info("replace: old key: {}, old value: {}, new value: {}",
                entry.key(), entry.value(), newValue);
        entry.doReplaceValue(newValue);
        return null;
    }
}
