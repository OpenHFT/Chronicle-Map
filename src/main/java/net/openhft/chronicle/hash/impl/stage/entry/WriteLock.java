/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.hash.impl.LocalLockState.UPDATE_LOCKED;
import static net.openhft.chronicle.hash.impl.LocalLockState.WRITE_LOCKED;

@Staged
public class WriteLock implements InterProcessLock {

    @StageRef SegmentStages s;
    
    @Override
    public boolean isHeldByCurrentThread() {
        return s.localLockState.write;
    }

    @Override
    public void lock() {
        switch (s.localLockState) {
            case UNLOCKED:
                if (s.writeZero()) {
                    if (!s.updateZero()) {
                        s.segmentHeader.upgradeUpdateToWriteLock(s.segmentHeaderAddress);
                    } else {
                        if (!s.readZero())
                            throw forbiddenWriteLockWhenOuterContextReadLocked();
                        s.segmentHeader.writeLock(s.segmentHeaderAddress);
                    }
                }
                s.incrementWrite();
                s.setLocalLockState(WRITE_LOCKED);
                return;
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
                if (s.writeZero()) {
                    assert !s.updateZero();
                    s.segmentHeader.upgradeUpdateToWriteLock(s.segmentHeaderAddress);
                }
                s.decrementUpdate();
                s.incrementWrite();
                s.setLocalLockState(WRITE_LOCKED);
            case WRITE_LOCKED:
                // do nothing
        }
    }

    /**
     * Non-static because after compilation it becomes inner class which forbids static methods
     */
    @NotNull
    private IllegalMonitorStateException forbiddenUpgrade() {
        return new IllegalMonitorStateException("Cannot upgrade from read to write lock");
    }

    /**
     * Non-static because after compilation it becomes inner class which forbids static methods
     */
    @NotNull
    private IllegalStateException forbiddenWriteLockWhenOuterContextReadLocked() {
        return new IllegalStateException("Cannot acquire write lock, because outer context " +
                "holds read lock. In this case you should acquire update lock in the outer " +
                "context up front");
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        switch (s.localLockState) {
            case UNLOCKED:
                if (s.writeZero()) {
                    if (!s.updateZero()) {
                        s.segmentHeader.upgradeUpdateToWriteLockInterruptibly(
                                s.segmentHeaderAddress);
                    } else {
                        if (!s.readZero())
                            throw forbiddenWriteLockWhenOuterContextReadLocked();
                        s.segmentHeader.writeLockInterruptibly(s.segmentHeaderAddress);
                    }
                }
                s.incrementWrite();
                s.setLocalLockState(WRITE_LOCKED);
                return;
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
                if (s.writeZero()) {
                    assert !s.updateZero();
                    s.segmentHeader.upgradeUpdateToWriteLockInterruptibly(s.segmentHeaderAddress);
                }
                s.decrementUpdate();
                s.incrementWrite();
                s.setLocalLockState(WRITE_LOCKED);
            case WRITE_LOCKED:
                // do nothing
        }
    }

    @Override
    public boolean tryLock() {
        switch (s.localLockState) {
            case UNLOCKED:
                if (s.writeZero()) {
                    if (!s.updateZero()) {
                        if (s.segmentHeader.tryUpgradeUpdateToWriteLock(s.segmentHeaderAddress)) {
                            s.incrementWrite();
                            s.setLocalLockState(WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        if (!s.readZero())
                            throw forbiddenWriteLockWhenOuterContextReadLocked();
                        if (s.segmentHeader.tryWriteLock(s.segmentHeaderAddress)) {
                            s.incrementWrite();
                            s.setLocalLockState(WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    }
                } else {
                    s.incrementWrite();
                    s.setLocalLockState(WRITE_LOCKED);
                    return true;
                }
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
                if (s.writeZero()) {
                    assert !s.updateZero();
                    if (s.segmentHeader.tryUpgradeUpdateToWriteLock(s.segmentHeaderAddress)) {
                        s.decrementUpdate();
                        s.incrementWrite();
                        s.setLocalLockState(WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    s.decrementUpdate();
                    s.incrementWrite();
                    s.setLocalLockState(WRITE_LOCKED);
                    return true;
                }
            case WRITE_LOCKED:
                return true;
        }
        throw new AssertionError();
    }

    @Override
    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
        switch (s.localLockState) {
            case UNLOCKED:
                if (s.writeZero()) {
                    if (!s.updateZero()) {
                        if (s.segmentHeader.tryUpgradeUpdateToWriteLock(
                                s.segmentHeaderAddress, time, unit)) {
                            s.incrementWrite();
                            s.setLocalLockState(WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        if (!s.readZero())
                            throw forbiddenWriteLockWhenOuterContextReadLocked();
                        if (s.segmentHeader.tryWriteLock(s.segmentHeaderAddress, time, unit)) {
                            s.incrementWrite();
                            s.setLocalLockState(WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    }
                } else {
                    s.incrementWrite();
                    s.setLocalLockState(WRITE_LOCKED);
                    return true;
                }
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
                if (s.writeZero()) {
                    assert !s.updateZero();
                    if (s.segmentHeader.tryUpgradeUpdateToWriteLock(
                            s.segmentHeaderAddress, time, unit)) {
                        s.decrementUpdate();
                        s.incrementWrite();
                        s.setLocalLockState(WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    s.decrementUpdate();
                    s.incrementWrite();
                    s.setLocalLockState(WRITE_LOCKED);
                    return true;
                }
            case WRITE_LOCKED:
                return true;
        }
        throw new AssertionError();
    }

    @Override
    public void unlock() {
        switch (s.localLockState) {
            case UNLOCKED:
            case READ_LOCKED:
            case UPDATE_LOCKED:
                return;
            case WRITE_LOCKED:
                int newTotalWriteLockCount = s.decrementWrite();
                if (newTotalWriteLockCount == 0) {
                    s.segmentHeader.downgradeWriteToUpdateLock(s.segmentHeaderAddress);
                } else {
                    assert newTotalWriteLockCount > 0 : "write underflow";
                }
        }
        s.incrementUpdate();
        s.setLocalLockState(UPDATE_LOCKED);
    }
}
