/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.ret;

import net.openhft.chronicle.map.ReturnValue;

/**
 * Internal extension of {@link ReturnValue} that allows the captured value to
 * be retrieved as a Java object.
 * <p>
 * Chronicle-Map staged code passes an {@code InstanceReturnValue} into map
 * operations so that they can populate the return value via
 * {@link #returnValue(net.openhft.chronicle.hash.Data)} and later read the
 * materialised instance through {@link #returnValue()}. Implementations are
 * responsible for deciding whether the returned instance is reused or freshly
 * allocated.
 *
 * @param <V> value type stored in the map
 */
public interface InstanceReturnValue<V> extends ReturnValue<V> {
    V returnValue();
}
