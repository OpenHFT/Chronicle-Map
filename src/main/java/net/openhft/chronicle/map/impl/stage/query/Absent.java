//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.set.SetAbsentEntry;
import org.jetbrains.annotations.NotNull;

public interface Absent<K, V> extends MapAbsentEntry<K, V>, SetAbsentEntry<K> {

    @NotNull
    @Override
    MapAndSetContext<K, V, ?> context();
}
