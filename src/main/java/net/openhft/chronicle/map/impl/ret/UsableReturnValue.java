/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.ret;

/**
 * {@link InstanceReturnValue} variant that can reuse a caller-supplied
 * instance when materialising the result of a map operation.
 * <p>
 * The {@link #initUsingReturnValue(Object)} method provides a mutable instance
 * that implementations may fill in-place when {@link #returnValue()} is
 * invoked. If no reusable instance is configured a new object may be created
 * instead. This pattern avoids allocations on hot paths such as
 * {@code acquireUsing}.
 *
 * @param <V> value type stored in the map
 */
public interface UsableReturnValue<V> extends InstanceReturnValue<V> {
    Object USING_RETURN_VALUE_UNINIT = new Object();

    void initUsingReturnValue(V usingValue);
}
