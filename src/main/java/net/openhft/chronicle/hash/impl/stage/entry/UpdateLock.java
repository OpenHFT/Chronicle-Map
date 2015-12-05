/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.hash.impl.LocalLockState.READ_LOCKED;
import static net.openhft.chronicle.hash.impl.LocalLockState.UPDATE_LOCKED;

@Staged
public class UpdateLock implements InterProcessLock {

    @StageRef LogHolder logHolder;
    @StageRef SegmentStages s;
    @StageRef HashEntryStages<?> entry;
    
    @Override
    public boolean isHeldByCurrentThread() {
        return s.localLockState.update;
    }

    @Override
    public void lock() {
        switch (s.localLockState) {
            case UNLOCKED:
                s.checkIterationContextNotLockedInThisThread();
                if (s.updateZero() && s.writeZero()) {
                    if (!s.readZero())
                        throw forbiddenUpdateLockWhenOuterContextReadLocked();
                    try {
                        s.segmentHeader.updateLock(s.segmentHeaderAddress);
                    } catch (RuntimeException e) {
                        logHolder.LOG.error(s.debugContextsAndLocks());
                        throw e;
                    }
                }
                s.incrementUpdate();
                s.setLocalLockState(UPDATE_LOCKED);
                return;
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
            case WRITE_LOCKED:
                // do nothing
        }
    }

    /**
     * Non-static because after compilation it becomes inner class which forbids static methods
     */
    @NotNull
    private IllegalMonitorStateException forbiddenUpgrade() {
        return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
    }

    /**
     * Non-static because after compilation it becomes inner class which forbids static methods
     */
    @NotNull
    private IllegalStateException forbiddenUpdateLockWhenOuterContextReadLocked() {
        return new IllegalStateException("Cannot acquire update lock, because outer context " +
                "holds read lock. In this case you should acquire update lock in the outer " +
                "context up front");
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        switch (s.localLockState) {
            case UNLOCKED:
                s.checkIterationContextNotLockedInThisThread();
                if (s.updateZero() && s.writeZero()) {
                    if (!s.readZero())
                        throw forbiddenUpdateLockWhenOuterContextReadLocked();
                    s.segmentHeader.updateLockInterruptibly(s.segmentHeaderAddress);
                }
                s.incrementUpdate();
                s.setLocalLockState(UPDATE_LOCKED);
                return;
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
            case WRITE_LOCKED:
                // do nothing
        }
    }

    @Override
    public boolean tryLock() {
        switch (s.localLockState) {
            case UNLOCKED:
                s.checkIterationContextNotLockedInThisThread();
                if (s.updateZero() && s.writeZero()) {
                    if (!s.readZero())
                        throw forbiddenUpdateLockWhenOuterContextReadLocked();
                    if (s.segmentHeader.tryUpdateLock(s.segmentHeaderAddress)) {
                        s.incrementUpdate();
                        s.setLocalLockState(UPDATE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    s.incrementUpdate();
                    s.setLocalLockState(UPDATE_LOCKED);
                    return true;
                }
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
            case WRITE_LOCKED:
                return true;
        }
        throw new AssertionError();
    }

    @Override
    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
        switch (s.localLockState) {
            case UNLOCKED:
                s.checkIterationContextNotLockedInThisThread();
                if (s.updateZero() && s.writeZero()) {
                    if (!s.readZero())
                        throw forbiddenUpdateLockWhenOuterContextReadLocked();
                    if (s.segmentHeader.tryUpdateLock(s.segmentHeaderAddress, time, unit)) {
                        s.incrementUpdate();
                        s.setLocalLockState(UPDATE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    s.incrementUpdate();
                    s.setLocalLockState(UPDATE_LOCKED);
                    return true;
                }
            case READ_LOCKED:
                throw forbiddenUpgrade();
            case UPDATE_LOCKED:
            case WRITE_LOCKED:
                return true;
        }
        throw new AssertionError();
    }

    @Override
    public void unlock() {
        entry.closeDelayedUpdateChecksum();
        switch (s.localLockState) {
            case UNLOCKED:
            case READ_LOCKED:
                return;
            case UPDATE_LOCKED:
                int newTotalUpdateLockCount = s.decrementUpdate();
                if (newTotalUpdateLockCount == 0) {
                    if (s.writeZero())
                        s.segmentHeader.downgradeUpdateToReadLock(s.segmentHeaderAddress);
                } else {
                    assert newTotalUpdateLockCount > 0 : "update underflow";
                }
                break;
            case WRITE_LOCKED:
                int newTotalWriteLockCount = s.decrementWrite();
                if (newTotalWriteLockCount == 0) {
                    if (!s.updateZero()) {
                        s.segmentHeader.downgradeWriteToUpdateLock(s.segmentHeaderAddress);
                    } else {
                        s.segmentHeader.downgradeWriteToReadLock(s.segmentHeaderAddress);
                    }
                } else {
                    assert newTotalWriteLockCount > 0 : "write underflow";
                }
        }
        s.incrementRead();
        s.setLocalLockState(READ_LOCKED);
    }
}
