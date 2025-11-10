//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.set.SetContext;

public interface MapAndSetContext<K, V, R> extends MapContext<K, V, R>, SetContext<K, R> {
}
