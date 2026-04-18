/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.ret;

import net.openhft.chronicle.map.ReturnValue;

public interface InstanceReturnValue<V> extends ReturnValue<V> {
    V returnValue();
}
