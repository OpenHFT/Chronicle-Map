/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.Range;
import net.openhft.chronicle.values.Values;

/**
 * <h2>This interface is a private API, don't use in client code</h2>
 * <p>
 * Excluding global mutable state lock
 */
public interface VanillaGlobalMutableState extends Byteable {

    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(VanillaGlobalMutableState.class);
    }

    @Group(1)
    int getAllocatedExtraTierBulks();

    void setAllocatedExtraTierBulks(@Range(min = 0, max = 16777215) int allocatedExtraTierBulks);

    @Group(2)
    long getFirstFreeTierIndex();

    void setFirstFreeTierIndex(@Range(min = 0, max = 1099511627775L) long firstFreeTierIndex);

    @Group(3)
    long getExtraTiersInUse();

    void setExtraTiersInUse(@Range(min = 0, max = 1099511627775L) long extraTiersInUse);

    @Group(4)
    long getSegmentHeadersOffset();

    void setSegmentHeadersOffset(@Range(min = 0, max = 4294967295L) long segmentHeadersOffset);

    @Group(5)
    long getDataStoreSize();

    void setDataStoreSize(@Range(min = 0, max = Long.MAX_VALUE) long dataStoreSize);

    long addDataStoreSize(long addition);
}
