/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.ret;

public interface UsableReturnValue<V> extends InstanceReturnValue<V> {
    Object USING_RETURN_VALUE_UNINIT = new Object();

    void initUsingReturnValue(V usingValue);
}
