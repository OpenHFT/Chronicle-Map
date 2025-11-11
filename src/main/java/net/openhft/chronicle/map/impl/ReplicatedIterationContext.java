/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

public interface ReplicatedIterationContext<K, V, R> extends IterationContext<K, V, R> {

    void readExistingEntry(long pos);
}
