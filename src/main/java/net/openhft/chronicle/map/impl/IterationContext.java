//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.map.ChronicleHashCorruptionImpl;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapSegmentContext;

public interface IterationContext<K, V, R> extends MapEntry<K, V>, MapSegmentContext<K, V, R> {
    long pos();

    void initSegmentIndex(int segmentIndex);

    void recoverSegments(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption);
}
