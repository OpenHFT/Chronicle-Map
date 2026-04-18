/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertPosition;

final class MemoryResource {
    final long address;
    final long size;

    MemoryResource(final long address, final long size) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(size);
        this.address = address;
        this.size = size;
    }
}
