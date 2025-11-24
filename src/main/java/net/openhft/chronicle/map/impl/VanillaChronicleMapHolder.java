/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Staged;

/**
 * Staged holder interface that exposes a {@link VanillaChronicleMap} and its
 * related views from within a compiled context.
 * <p>
 * Implementations created by the staging compiler bridge the generic
 * {@link net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder}
 * infrastructure to the map and optional {@link ChronicleSet} overlay, giving
 * staged code a strongly typed handle on the underlying data structure.
 * <p>
 * This interface is internal to Chronicle-Map and must not be used directly in
 * user code.
 */
@Staged
public interface VanillaChronicleMapHolder<K, V, R> extends VanillaChronicleHashHolder<K> {

    VanillaChronicleMap<K, V, R> m();

    @Override
    VanillaChronicleHash<K, ?, ?, ?> h();

    ChronicleMap<K, V> map();

    ChronicleSet<K> set();
}
