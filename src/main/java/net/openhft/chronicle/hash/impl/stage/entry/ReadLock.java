/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.hash.impl.LocalLockState.READ_LOCKED;
import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;

@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public class ReadLock implements InterProcessLock {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    SegmentStages s;
    @StageRef
    HashEntryStages entry;
    @StageRef
    HashLookupPos hlp;

    @Override
    public boolean isHeldByCurrentThread() {
        checkOnEachPublicOperation.checkOnEachLockOperation();
        return s.localLockState.read;
    }

    @Override
    public void lock() {
        checkOnEachPublicOperation.checkOnEachLockOperation();
        if (s.localLockState == UNLOCKED) {
            if (s.readZero() && s.updateZero() && s.writeZero()) {
                try {
                    s.segmentHeader.readLock(s.segmentHeaderAddress);
                } catch (InterProcessDeadLockException e) {
                    throw s.debugContextsAndLocks(e);
                }
            }
            s.incrementRead();
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        checkOnEachPublicOperation.checkOnEachLockOperation();
        if (Thread.interrupted())
            throw new InterruptedException();
        if (s.localLockState == UNLOCKED) {
            if (s.readZero() && s.updateZero() && s.writeZero()) {
                try {
                    s.segmentHeader.readLockInterruptibly(s.segmentHeaderAddress);
                } catch (InterProcessDeadLockException e) {
                    throw s.debugContextsAndLocks(e);
                }
            }
            s.incrementRead();
            s.setLocalLockState(READ_LOCKED);
        }
    }

    @Override
    public boolean tryLock() {
        checkOnEachPublicOperation.checkOnEachLockOperation();
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
        checkOnEachPublicOperation.checkOnEachLockOperation();
        if (Thread.interrupted())
            throw new InterruptedException();
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
        checkOnEachPublicOperation.checkOnEachLockOperation();
        if (s.localLockState != UNLOCKED) {
            // TODO what should close here?
            hlp.closeHashLookupPos();
            entry.closeEntry();
        }
        s.readUnlockAndDecrementCount();
        s.setLocalLockState(UNLOCKED);
    }

    @Override
    public boolean isHeld() {
        return s.localLockState != null &&
                s.localLockState != UNLOCKED;
    }
}
