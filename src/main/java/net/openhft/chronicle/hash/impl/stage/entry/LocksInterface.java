/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

/**
 * Abstraction over per-segment lock accounting shared across contexts.
 *
 * <p>Implementations track the current segment header, aggregate read, update and
 * write lock counts across all contexts on the segment, and form a linked list
 * of contexts participating in locking so that diagnostics and deadlock reports
 * can enumerate their state.
 */
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
