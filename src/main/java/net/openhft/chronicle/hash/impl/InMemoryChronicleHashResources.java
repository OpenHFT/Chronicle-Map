//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.OS;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertPosition;

public final class InMemoryChronicleHashResources extends ChronicleHashResources {
    @Override
    void releaseMemoryResource(final MemoryResource allocation) {
        assert SKIP_ASSERTIONS || assertAddress(allocation.address);
        assert SKIP_ASSERTIONS || assertPosition(allocation.size);
        OS.memory().freeMemory(allocation.address, allocation.size);
    }
}
