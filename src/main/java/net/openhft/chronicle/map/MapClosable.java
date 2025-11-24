/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.ManagedCloseable;

/**
 * Marker interface for Chronicle-Map resources that can be closed.
 * <p>
 * Implementations may not be able to report an accurate closed state
 * in all environments; the default {@link #isClosed()} therefore
 * returns {@code false} so that callers treat the instance as live
 * unless explicitly closed.
 */
public interface MapClosable extends ManagedCloseable {

    @Override
    default boolean isClosed() {
        // if we do not know, pretend it is not
        return false;
    }
}
