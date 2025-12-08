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

    long entrySize(long keySize, long valueSize);
}
