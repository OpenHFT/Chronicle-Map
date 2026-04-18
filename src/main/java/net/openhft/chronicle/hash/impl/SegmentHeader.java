/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl;

import java.util.concurrent.TimeUnit;

public interface SegmentHeader {
    long entries(long address);

    void entries(long address, long size);

    long deleted(long address);

    void deleted(long address, long deleted);

    long lowestPossiblyFreeChunk(long address);

    void lowestPossiblyFreeChunk(long address, long lowestPossiblyFreeChunk);

    long nextTierIndex(long address);

    void nextTierIndex(long address, long nextTierIndex);

    void readLock(long address);

    void readLockInterruptibly(long address) throws InterruptedException;

    boolean tryReadLock(long address);

    boolean tryReadLock(long address, long time, TimeUnit unit) throws InterruptedException;

    void updateLock(long address);

    void updateLockInterruptibly(long address) throws InterruptedException;

    boolean tryUpdateLock(long address);

    boolean tryUpdateLock(long address, long time, TimeUnit unit) throws InterruptedException;

    void writeLock(long address);

    void writeLockInterruptibly(long address) throws InterruptedException;

    boolean tryWriteLock(long address);

    boolean tryWriteLock(long address, long time, TimeUnit unit) throws InterruptedException;

    boolean tryUpgradeReadToUpdateLock(long address);

    boolean tryUpgradeReadToWriteLock(long address);

    void upgradeUpdateToWriteLock(long address);

    void upgradeUpdateToWriteLockInterruptibly(long address) throws InterruptedException;

    boolean tryUpgradeUpdateToWriteLock(long address);

    boolean tryUpgradeUpdateToWriteLock(long address, long time, TimeUnit unit)
            throws InterruptedException;

    void readUnlock(long address);

    void updateUnlock(long address);

    void downgradeUpdateToReadLock(long address);

    void writeUnlock(long address);

    void downgradeWriteToUpdateLock(long address);

    void downgradeWriteToReadLock(long address);

    void resetLock(long address);

    long resetLockState();

    long getLockState(long address);

    String lockStateToString(long lockState);
}
