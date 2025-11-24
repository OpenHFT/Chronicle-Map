/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.NotNull;

/**
 * Default implementation of all Chronicle-Map SPI interfaces.
 * <p>
 * This singleton provides baseline implementations of {@link MapMethods},
 * {@link MapEntryOperations}, {@link MapRemoteOperations}, and
 * {@link DefaultValueProvider}. Builders use it when callers have not supplied
 * custom strategies, keeping the configuration surface small while still
 * allowing advanced users to plug in specialised behaviour.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
final class DefaultSpi implements MapMethods, MapEntryOperations, MapRemoteOperations,
        DefaultValueProvider {
    static final DefaultSpi DEFAULT_SPI = new DefaultSpi();

    static <K, V, R> MapMethods<K, V, R> mapMethods() {
        return DEFAULT_SPI;
    }

    static <K, V, R> MapEntryOperations<K, V, R> mapEntryOperations() {
        return DEFAULT_SPI;
    }

    static <K, V, R> MapRemoteOperations<K, V, R> mapRemoteOperations() {
        return DEFAULT_SPI;
    }

    static <K, V> DefaultValueProvider<K, V> defaultValueProvider() {
        return DEFAULT_SPI;
    }

    @Override
    public Data defaultValue(@NotNull MapAbsentEntry absentEntry) {
        return absentEntry.defaultValue();
    }
}
