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

import static net.openhft.chronicle.hash.impl.LocalLockState.READ_LOCKED;
import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;


@Staged
public class ReadLock implements InterProcessLock {
    
    @StageRef SegmentStages s;
    @StageRef HashEntryStages entry;
    @StageRef HashLookupPos hlp;
    
    @Override
    public boolean isHeldByCurrentThread() {
        return s.localLockState.read;
    }

    @Override
    public void lock() {
        if (s.localLockState == UNLOCKED) {
            s.segmentHeader.readLock(s.segmentHeaderAddress);
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (s.localLockState == UNLOCKED) {
            s.segmentHeader.readLockInterruptibly(s.segmentHeaderAddress);
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public boolean tryLock() {
        if (s.localLockState == UNLOCKED) {
            if (s.segmentHeader.tryReadLock(s.segmentHeaderAddress)) {
                s.setLocalLockState(READ_LOCKED);
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
        if (s.localLockState == UNLOCKED) {
            if (s.segmentHeader.tryReadLock(s.segmentHeaderAddress, time, unit)) {
                s.setLocalLockState(READ_LOCKED);
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public void unlock() {
        switch (s.localLockState) {
            case UNLOCKED:
                return;
            case READ_LOCKED:
                s.segmentHeader.readUnlock(s.segmentHeaderAddress);
                break;
            case UPDATE_LOCKED:
                s.segmentHeader.updateUnlock(s.segmentHeaderAddress);
                break;
            case WRITE_LOCKED:
                s.segmentHeader.writeUnlock(s.segmentHeaderAddress);
        }
        s.setLocalLockState(UNLOCKED);
        // TODO what should close here?
        hlp.closeHashLookupPos();
        entry.closePos();
    }
}
