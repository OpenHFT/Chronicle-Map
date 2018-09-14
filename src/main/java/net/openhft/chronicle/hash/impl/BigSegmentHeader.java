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

import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.algo.bytes.NativeAccess;
import net.openhft.chronicle.algo.locks.VanillaReadWriteUpdateWithWaitsLockingStrategy;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.threads.ThreadHints;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class BigSegmentHeader implements SegmentHeader {
    public static final BigSegmentHeader INSTANCE = new BigSegmentHeader();
    static final long LOCK_OFFSET = 0L;
    static final long ENTRIES_OFFSET = LOCK_OFFSET + 8L; // 32-bit
    static final long LOWEST_POSSIBLY_FREE_CHUNK_OFFSET = ENTRIES_OFFSET + 4L;
    static final long NEXT_TIER_INDEX_OFFSET = LOWEST_POSSIBLY_FREE_CHUNK_OFFSET + 4L;
    static final long DELETED_OFFSET = NEXT_TIER_INDEX_OFFSET + 8L;
    private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;
    /**
     * Make the LOCK constant and {@link #A} of final class types (instead of interfaces) as this
     * hopefully help JVM with inlining
     */
    private static final VanillaReadWriteUpdateWithWaitsLockingStrategy LOCK =
            (VanillaReadWriteUpdateWithWaitsLockingStrategy)
                    VanillaReadWriteUpdateWithWaitsLockingStrategy.instance();
    private static final NativeAccess A = (NativeAccess) Access.nativeAccess();
    private static final int TRY_LOCK_NANOS_THRESHOLD = 2_000_000;

    public static int LOCK_TIMEOUT_SECONDS;

    static {
        // Previously this value was 2 seconds, but GC pauses often take more time that shouldn't
        // result to InterProcessDeadLockException.
        int timeout = 60;
        try {
            timeout = Integer.parseInt(System.getProperty("net.openhft.chronicle.map.lockTimeoutSeconds", String.valueOf(timeout)));
        } catch (NumberFormatException ex) {
            // ignore
        }

        LOCK_TIMEOUT_SECONDS = timeout;
    }

    private BigSegmentHeader() {
    }

    private static InterProcessDeadLockException deadLock() {
        return new InterProcessDeadLockException(
                "Failed to acquire the lock in " + LOCK_TIMEOUT_SECONDS + " seconds.\n" +
                        "Possible reasons:\n" +
                        " - The lock was not released by the previous holder. If you use contexts API,\n" +
                        " for example map.queryContext(key), in a try-with-resources block.\n" +
                        " - This Chronicle Map (or Set) instance is persisted to disk, and the previous\n" +
                        " process (or one of parallel accessing processes) has crashed while holding\n" +
                        " this lock. In this case you should use ChronicleMapBuilder.recoverPersistedTo()" +
                        " procedure\n" +
                        " to access the Chronicle Map instance.\n" +
                        " - A concurrent thread or process, currently holding this lock, spends\n" +
                        " unexpectedly long time (more than " + LOCK_TIMEOUT_SECONDS + " seconds) in\n" +
                        " the context (try-with-resource block) or one of overridden interceptor\n" +
                        " methods (or MapMethods, or MapEntryOperations, or MapRemoteOperations)\n" +
                        " while performing an ordinary Map operation or replication. You should either\n" +
                        " redesign your logic to spend less time in critical sections (recommended) or\n" +
                        " acquire this lock with tryLock(time, timeUnit) method call, with sufficient\n" +
                        " time specified.\n" +
                        " - Segment(s) in your Chronicle Map are very large, and iteration over them\n" +
                        " takes more than " + LOCK_TIMEOUT_SECONDS + " seconds. In this case you should\n" +
                        " acquire this lock with tryLock(time, timeUnit) method call, with longer\n" +
                        " timeout specified.\n" +
                        " - This is a dead lock. If you perform multi-key queries, ensure you acquire\n" +
                        " segment locks in the order (ascending by segmentIndex()), you can find\n" +
                        " an example here: https://github.com/OpenHFT/Chronicle-Map#multi-key-queries\n");
    }

    private static long roundUpNanosToMillis(long nanos) {
        return NANOSECONDS.toMillis(nanos + 900_000);
    }

    private static boolean innerTryReadLock(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        return LOCK.tryReadLock(A, null, address + LOCK_OFFSET) ||
                tryReadLock0(address, time, unit, interruptible);
    }

    private static boolean tryReadLock0(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryReadLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryReadLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryReadLockNanos(long address, long timeInNanos, boolean interruptible)
            throws InterruptedException {
        long end = System.nanoTime() + timeInNanos;
        do {
            if (LOCK.tryReadLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            ThreadHints.onSpinWait();
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryReadLockMillis(long address, long timeInMillis, boolean interruptible)
            throws InterruptedException {
        long lastTime = System.currentTimeMillis();
        do {
            if (LOCK.tryReadLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            ThreadHints.onSpinWait();
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    private static boolean innerTryUpdateLock(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        return LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET) ||
                tryUpdateLock0(address, time, unit, interruptible);
    }

    private static boolean tryUpdateLock0(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpdateLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryUpdateLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryUpdateLockNanos(long address, long timeInNanos, boolean interruptible)
            throws InterruptedException {
        long end = System.nanoTime() + timeInNanos;
        do {
            if (LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            ThreadHints.onSpinWait();
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryUpdateLockMillis(
            long address, long timeInMillis, boolean interruptible) throws InterruptedException {
        long lastTime = System.currentTimeMillis();
        do {
            if (LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            ThreadHints.onSpinWait();
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    private static boolean innerTryWriteLock(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        return LOCK.tryWriteLock(A, null, address + LOCK_OFFSET) ||
                tryWriteLock0(address, time, unit, interruptible);
    }

    private static boolean tryWriteLock0(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryWriteLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryWriteLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryWriteLockNanos(long address, long timeInNanos, boolean interruptible)
            throws InterruptedException {
        long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        try {
            do {
                if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                    return true;
                checkInterrupted(interruptible);
                ThreadHints.onSpinWait();
            } while (System.nanoTime() <= end);
            deregisterWait(address);
            return false;
        } catch (Throwable t) {
            throw tryDeregisterWaitAndRethrow(address, t);
        }
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryWriteLockMillis(
            long address, long timeInMillis, boolean interruptible) throws InterruptedException {
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        try {
            do {
                if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                    return true;
                checkInterrupted(interruptible);
                ThreadHints.onSpinWait();
                long now = System.currentTimeMillis();
                if (now != lastTime) {
                    lastTime = now;
                    timeInMillis--;
                }
            } while (timeInMillis >= 0);
            deregisterWait(address);
            return false;
        } catch (Throwable t) {
            throw tryDeregisterWaitAndRethrow(address, t);
        }
    }

    private static void checkInterrupted(boolean interruptible) throws InterruptedException {
        if (interruptible && Thread.interrupted())
            throw new InterruptedException();
    }

    private static void registerWait(long address) {
        LOCK.registerWait(A, null, address + LOCK_OFFSET);
    }

    private static void deregisterWait(long address) {
        LOCK.deregisterWait(A, null, address + LOCK_OFFSET);
    }

    private static boolean innerTryUpgradeUpdateToWriteLock(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        return LOCK.tryUpgradeUpdateToWriteLock(A, null, address + LOCK_OFFSET) ||
                tryUpgradeUpdateToWriteLock0(address, time, unit, interruptible);
    }

    private static boolean tryUpgradeUpdateToWriteLock0(
            long address, long time, TimeUnit unit, boolean interruptible)
            throws InterruptedException {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpgradeUpdateToWriteLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryUpgradeUpdateToWriteLockMillis(
                    address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryUpgradeUpdateToWriteLockNanos(
            long address, long timeInNanos, boolean interruptible) throws InterruptedException {
        long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        try {
            do {
                if (tryUpgradeUpdateToWriteLockAndDeregisterWait0(address))
                    return true;
                checkInterrupted(interruptible);
                ThreadHints.onSpinWait();
            } while (System.nanoTime() <= end);
            deregisterWait(address);
            return false;
        } catch (Throwable t) {
            throw tryDeregisterWaitAndRethrow(address, t);
        }
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryUpgradeUpdateToWriteLockMillis(
            long address, long timeInMillis, boolean interruptible) throws InterruptedException {
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        try {
            do {
                if (tryUpgradeUpdateToWriteLockAndDeregisterWait0(address))
                    return true;
                checkInterrupted(interruptible);
                ThreadHints.onSpinWait();
                long now = System.currentTimeMillis();
                if (now != lastTime) {
                    lastTime = now;
                    timeInMillis--;
                }
            } while (timeInMillis >= 0);
            deregisterWait(address);
            return false;
        } catch (Throwable t) {
            throw tryDeregisterWaitAndRethrow(address, t);
        }
    }

    private static RuntimeException tryDeregisterWaitAndRethrow(long address, Throwable throwable) {
        try {
            deregisterWait(address);
        } catch (Throwable t) {
            throwable.addSuppressed(t);
        }
        throw Jvm.rethrow(throwable);
    }

    private static boolean tryUpgradeUpdateToWriteLockAndDeregisterWait0(long address) {
        return LOCK.tryUpgradeUpdateToWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET);
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
    public long nextTierIndex(long address) {
        return OS.memory().readLong(address + NEXT_TIER_INDEX_OFFSET);
    }

    @Override
    public void nextTierIndex(long address, long nextTierIndex) {
        OS.memory().writeLong(address + NEXT_TIER_INDEX_OFFSET, nextTierIndex);
    }

    @Override
    public void readLock(long address) {
        try {
            if (!innerTryReadLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void readLockInterruptibly(long address) throws InterruptedException {
        if (!tryReadLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryReadLock(long address) {
        return LOCK.tryReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryReadLock(long address, long time, TimeUnit unit) throws InterruptedException {
        return innerTryReadLock(address, time, unit, true);
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
        try {
            if (!innerTryUpdateLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void updateLockInterruptibly(long address) throws InterruptedException {
        if (!tryUpdateLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryUpdateLock(long address) {
        return LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpdateLock(long address, long time, TimeUnit unit)
            throws InterruptedException {
        return innerTryUpdateLock(address, time, unit, true);
    }

    @Override
    public void writeLock(long address) {
        try {
            if (!innerTryWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void writeLockInterruptibly(long address) throws InterruptedException {
        if (!tryWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryWriteLock(long address) {
        return LOCK.tryWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryWriteLock(long address, long time, TimeUnit unit)
            throws InterruptedException {
        return innerTryWriteLock(address, time, unit, true);
    }

    @Override
    public void upgradeUpdateToWriteLock(long address) {
        try {
            if (!innerTryUpgradeUpdateToWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void upgradeUpdateToWriteLockInterruptibly(long address) throws InterruptedException {
        if (!tryUpgradeUpdateToWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(long address) {
        return LOCK.tryUpgradeUpdateToWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(long address, long time, TimeUnit unit)
            throws InterruptedException {
        return innerTryUpgradeUpdateToWriteLock(address, time, unit, true);
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
