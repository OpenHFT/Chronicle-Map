/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.sg.Staged;

@Staged
public interface ReplicatedChronicleMapHolder<K, V, R> extends VanillaChronicleMapHolder<K, V, R> {

    @Override
    ReplicatedChronicleMap<K, V, R> m();
}
