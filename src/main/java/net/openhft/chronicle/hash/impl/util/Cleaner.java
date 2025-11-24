/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

/**
 * Lightweight callback interface for releasing native or off-heap resources.
 *
 * <p>Instances are typically created via {@link CleanerUtils} and encapsulate the logic
 * required to clean up underlying buffers or file mappings once they are no longer in use.
 */
public interface Cleaner {

    void clean();

}
