/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.set.SetContext;

/**
 * Internal marker interface for staged contexts that can be viewed as both a
 * map and a set.
 * <p>
 * Chronicle-Map uses this abstraction for shared implementations that back a
 * {@code ChronicleMap} together with a {@code ChronicleSet} overlay, allowing
 * query code to treat the two views uniformly.
 */
public interface MapAndSetContext<K, V, R> extends MapContext<K, V, R>, SetContext<K, R> {
}
