/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.values.Group;

/**
 * Value interface backing a stamped lock used by Chronicle-Map.
 * <p>
 * The fields defined here are implemented by Chronicle Values to
+ * provide a compact, off-heap friendly representation of the lock
 * state associated with a map entry or segment.
 */
interface ChronicleStampedLockVOInterface {

    @Group(0)
    long getEntryLockState();

    void setEntryLockState(long entryLockState);
    //
    //    @Group(1)
    //    long getReaderCount();
    //
    //    void setReaderCount(long rc);  /* time in millis */
    //
    //    long addAtomicReaderCount(long toAdd);
    //

}
