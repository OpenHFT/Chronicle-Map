/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.values.Group;

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
