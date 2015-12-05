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

import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
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
            if (s.readZero() && s.updateZero() && s.writeZero())
                s.segmentHeader.readLock(s.segmentHeaderAddress);
            s.incrementRead();
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (s.localLockState == UNLOCKED) {
            if (s.readZero() && s.updateZero() && s.writeZero())
                s.segmentHeader.readLockInterruptibly(s.segmentHeaderAddress);
            s.incrementRead();
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public boolean tryLock() {
        if (s.localLockState == UNLOCKED) {
            if (!s.readZero() || !s.updateZero() || !s.writeZero() ||
                    s.segmentHeader.tryReadLock(s.segmentHeaderAddress)) {
                s.incrementRead();
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
            if (!s.readZero() || !s.updateZero() || !s.writeZero() ||
                    s.segmentHeader.tryReadLock(s.segmentHeaderAddress, time, unit)) {
                s.incrementRead();
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
        // TODO what should close here?
        hlp.closeHashLookupPos();
        entry.closePos();
        entry.closeKeyOffset();
        s.readUnlockAndDecrementCount();
        s.setLocalLockState(UNLOCKED);
    }
}
