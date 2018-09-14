/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
