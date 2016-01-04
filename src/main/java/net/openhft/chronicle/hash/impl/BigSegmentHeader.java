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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.algo.bytes.NativeAccess;
import net.openhft.chronicle.algo.locks.VanillaReadWriteUpdateWithWaitsLockingStrategy;
import net.openhft.chronicle.core.OS;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class BigSegmentHeader implements SegmentHeader {
    public static final BigSegmentHeader INSTANCE = new BigSegmentHeader();

    private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

    static final long LOCK_OFFSET = 0L;
    /**
     * Make the LOCK constant and {@link #A} of final class types (instead of interfaces) as this
     * hopefully help JVM with inlining
     */
    private static final VanillaReadWriteUpdateWithWaitsLockingStrategy LOCK =
            (VanillaReadWriteUpdateWithWaitsLockingStrategy)
            VanillaReadWriteUpdateWithWaitsLockingStrategy.instance();
    private static final NativeAccess A = (NativeAccess) Access.nativeAccess();

    static final long ENTRIES_OFFSET = LOCK_OFFSET + 8L; // 32-bit
    static final long LOWEST_POSSIBLY_FREE_CHUNK_OFFSET = ENTRIES_OFFSET + 4L;
    static final long DELETED_OFFSET = LOWEST_POSSIBLY_FREE_CHUNK_OFFSET + 4L;


    private static final int TRY_LOCK_NANOS_THRESHOLD = 2_000_000;

    private static long roundUpNanosToMillis(long nanos) {
        return NANOSECONDS.toMillis(nanos + 900_000);
    }

    private BigSegmentHeader() {
    }

    @Override
    public long entries(long address) {
        return OS.memory().readInt(address + ENTRIES_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void entries(long address, long entries) {
        if (entries >= (1L << 32)) {
            throw new IllegalStateException("segment entries overflow: up to " + UNSIGNED_INT_MASK +
                    " supported, " + entries + " given");
        }
        OS.memory().writeInt(address + ENTRIES_OFFSET, (int) entries);
    }

    @Override
    public long deleted(long address) {
        return OS.memory().readInt(address + DELETED_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void deleted(long address, long deleted) {
        if (deleted >= (1L << 32)) {
            throw new IllegalStateException("segment deleted entries count overflow: up to " +
                    UNSIGNED_INT_MASK + " supported, " + deleted + " given");
        }
        OS.memory().writeInt(address + DELETED_OFFSET, (int) deleted);
    }

    @Override
    public long lowestPossiblyFreeChunk(long address) {
        return OS.memory().readInt(address + LOWEST_POSSIBLY_FREE_CHUNK_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void lowestPossiblyFreeChunk(long address, long lowestPossiblyFreeChunk) {
        OS.memory().writeInt(address + LOWEST_POSSIBLY_FREE_CHUNK_OFFSET,
                (int) lowestPossiblyFreeChunk);
    }

    @Override
    public void readLock(long address) {
        if (!tryReadLock(address, 2, TimeUnit.SECONDS)) {
            throw new RuntimeException("Dead lock");
        }
    }

    @Override
    public void readLockInterruptibly(long address) {
        readLock(address);
    }

    @Override
    public boolean tryReadLock(long address) {
        return LOCK.tryReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryReadLock(long address, long time, TimeUnit unit) {
        return tryReadLock(address) || tryReadLock0(address, time, unit);
    }

    private boolean tryReadLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryReadLockNanos(address, timeInNanos);
        } else {
            return tryReadLockMillis(address, roundUpNanosToMillis(timeInNanos));
        }
    }

    private boolean tryReadLockNanos(long address, long timeInNanos) {
        long end = System.nanoTime() + timeInNanos;
        do {
            if (tryReadLock(address))
                return true;
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private boolean tryReadLockMillis(long address, long timeInMillis) {
        long lastTime = System.currentTimeMillis();
        do {
            if (tryReadLock(address))
                return true;
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    @Override
    public boolean tryUpgradeReadToUpdateLock(long address) {
        return LOCK.tryUpgradeReadToUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpgradeReadToWriteLock(long address) {
        return LOCK.tryUpgradeReadToWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void updateLock(long address) {
        if (!tryUpdateLock(address, 2, TimeUnit.SECONDS)) {
            throw new RuntimeException("Dead lock");
        }
    }

    @Override
    public void updateLockInterruptibly(long address) {
        updateLock(address);
    }

    @Override
    public boolean tryUpdateLock(long address) {
        return LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpdateLock(long address, long time, TimeUnit unit) {
        return tryUpdateLock(address) || tryUpdateLock0(address, time, unit);
    }

    private boolean tryUpdateLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpdateLockNanos(address, timeInNanos);
        } else {
            return tryUpdateLockMillis(address, roundUpNanosToMillis(timeInNanos));
        }
    }

    private boolean tryUpdateLockNanos(long address, long timeInNanos) {
        long end = System.nanoTime() + timeInNanos;
        do {
            if (tryUpdateLock(address))
                return true;
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private boolean tryUpdateLockMillis(long address, long timeInMillis) {
        long lastTime = System.currentTimeMillis();
        do {
            if (tryUpdateLock(address))
                return true;
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    @Override
    public void writeLock(long address) {
        if (!tryWriteLock(address, 2, TimeUnit.SECONDS)) {
            throw new RuntimeException("Dead lock");
        }
    }

    @Override
    public void writeLockInterruptibly(long address) {
        writeLock(address);
    }

    @Override
    public boolean tryWriteLock(long address) {
        return LOCK.tryWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryWriteLock(long address, long time, TimeUnit unit) {
        return tryWriteLock(address) || tryWriteLock0(address, time, unit);
    }

    private boolean tryWriteLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryWriteLockNanos(address, timeInNanos);
        } else {
            return tryWriteLockMillis(address, roundUpNanosToMillis(timeInNanos));
        }
    }

    private boolean tryWriteLockNanos(long address, long timeInNanos) {
        long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        do {
            if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                return true;
        } while (System.nanoTime() <= end);
        deregisterWait(address);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private boolean tryWriteLockMillis(long address, long timeInMillis) {
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        do {
            if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                return true;
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        deregisterWait(address);
        return false;
    }

    private static void registerWait(long address) {
        LOCK.registerWait(A, null, address + LOCK_OFFSET);
    }

    private static void deregisterWait(long address) {
        LOCK.deregisterWait(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void upgradeUpdateToWriteLock(long address) {
        if (!tryUpgradeUpdateToWriteLock(address, 2, TimeUnit.SECONDS)) {
            throw new RuntimeException("Dead lock");
        }
    }

    @Override
    public void upgradeUpdateToWriteLockInterruptibly(long address) {
        upgradeUpdateToWriteLock(address);
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(long address) {
        return LOCK.tryUpgradeUpdateToWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(long address, long time, TimeUnit unit) {
        return tryUpgradeUpdateToWriteLock(address) ||
                tryUpgradeUpdateToWriteLock0(address, time, unit);
    }

    private boolean tryUpgradeUpdateToWriteLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpgradeUpdateToWriteLockNanos(address, timeInNanos);
        } else {
            return tryUpgradeUpdateToWriteLockMillis(address, roundUpNanosToMillis(timeInNanos));
        }
    }

    private boolean tryUpgradeUpdateToWriteLockNanos(long address, long timeInNanos) {
        long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        do {
            if (LOCK.tryUpgradeUpdateToWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                return true;
        } while (System.nanoTime() <= end);
        deregisterWait(address);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private boolean tryUpgradeUpdateToWriteLockMillis(long address, long timeInMillis) {
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        do {
            if (LOCK.tryUpgradeUpdateToWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                return true;
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        deregisterWait(address);
        return false;
    }

    @Override
    public void readUnlock(long address) {
        LOCK.readUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void updateUnlock(long address) {
        LOCK.updateUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeUpdateToReadLock(long address) {
        LOCK.downgradeUpdateToReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void writeUnlock(long address) {
        LOCK.writeUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeWriteToUpdateLock(long address) {
        LOCK.downgradeWriteToUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeWriteToReadLock(long address) {
        LOCK.downgradeWriteToReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void resetLock(long address) {
        LOCK.reset(A, null, address + LOCK_OFFSET);
    }

    @Override
    public long resetLockState() {
        return LOCK.resetState();
    }

    @Override
    public long getLockState(long address) {
        return LOCK.getState(A, null, address + LOCK_OFFSET);
    }

    @Override
    public String lockStateToString(long lockState) {
        return LOCK.toString(lockState);
    }
}
