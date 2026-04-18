/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Staged;

@Staged
public interface VanillaChronicleMapHolder<K, V, R> extends VanillaChronicleHashHolder<K> {

    VanillaChronicleMap<K, V, R> m();

    @Override
    VanillaChronicleHash<K, ?, ?, ?> h();

    ChronicleMap<K, V> map();

    ChronicleSet<K> set();
}
