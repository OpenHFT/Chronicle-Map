//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

import net.openhft.sg.Staged;

@Staged
public interface VanillaChronicleHashHolder<K> {
    VanillaChronicleHash<K, ?, ?, ?> h();
}
