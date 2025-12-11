/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

/**
 * Minimal cleaner abstraction to release resources.
 */
public interface Cleaner {

    /**
     * Performs cleanup.
     */
    void clean();

}
