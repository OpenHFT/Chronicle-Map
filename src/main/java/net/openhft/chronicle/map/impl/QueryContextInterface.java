/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapClosable;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;

/**
 * Internal extension of {@link ExternalMapQueryContext} used by
 * {@link net.openhft.chronicle.map.VanillaChronicleMap} query pipelines.
 * <p>
 * Implementations expose the wiring between the compiled staged context and the
 * public {@code ChronicleMap} API: they provide access to the input key and
 * value {@link DataAccess} instances, the segment index being operated on and
 * the {@link InstanceReturnValue} strategies used to capture results.
 * <p>
 * Instances are obtained from the owning map and must be closed after use to
 * release off-heap resources. This interface is internal SPI and is not
 * intended for direct use in application code.
 */
public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {

    void initInputKey(Data<K> inputKey);

    Data<K> getInputKeyBytesAsData(BytesStore<?, ?> bytesStore, long offset, long size);

    DataAccess<K> inputKeyDataAccess();

    InstanceReturnValue<V> defaultReturnValue();

    UsableReturnValue<V> usingReturnValue();

    DataAccess<V> inputValueDataAccess();

    MapClosable acquireHandle();

    void initSegmentIndex(int segmentIndex);

    boolean segmentIndexInit();
}
