/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

public interface LocksInterface {

    boolean segmentHeaderInit();

    long segmentHeaderAddress();

    boolean locksInit();

    LocksInterface rootContextLockedOnThisSegment();

    void setNestedContextsLockedOnSameSegment(boolean nestedContextsLockedOnSameSegment);

    int latestSameThreadSegmentModCount();

    int changeAndGetLatestSameThreadSegmentModCount(int change);

    int totalReadLockCount();

    int changeAndGetTotalReadLockCount(int change);

    int totalUpdateLockCount();

    int changeAndGetTotalUpdateLockCount(int change);

    int totalWriteLockCount();

    int changeAndGetTotalWriteLockCount(int change);

    LocksInterface nextNode();

    void setNextNode(LocksInterface nextNode);

    String debugLocksState();
}
