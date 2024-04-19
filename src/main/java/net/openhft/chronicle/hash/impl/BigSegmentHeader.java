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
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;

@SuppressWarnings({"rawtypes", "unchecked"})
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
            timeout = Integer.parseInt(Jvm.getProperty("net.openhft.chronicle.map.lockTimeoutSeconds", String.valueOf(timeout)));
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
                        " an example here: https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial.adoc#multi-key-queries\n");
    }

    private static long roundUpNanosToMillis(long nanos) {
        return NANOSECONDS.toMillis(nanos + 900_000);
    }

    private static boolean innerTryReadLock(final long address,
                                            final long time,
                                            final TimeUnit unit,
                                            final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryReadLock(A, null, address + LOCK_OFFSET) ||
                tryReadLock0(address, time, unit, interruptible);
    }

    private static boolean tryReadLock0(final long address,
                                        final long time,
                                        final TimeUnit unit,
                                        final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryReadLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryReadLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryReadLockNanos(final long address,
                                            final long timeInNanos,
                                            final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long end = System.nanoTime() + timeInNanos;
        do {
            if (LOCK.tryReadLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            Jvm.nanoPause();
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryReadLockMillis(final long address,
                                             long timeInMillis,
                                             final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        long lastTime = System.currentTimeMillis();
        do {
            if (LOCK.tryReadLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            Jvm.nanoPause();
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    private static boolean innerTryUpdateLock(final long address,
                                              final long time,
                                              final TimeUnit unit,
                                              final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET) ||
                tryUpdateLock0(address, time, unit, interruptible);
    }

    private static boolean tryUpdateLock0(final long address,
                                          final long time,
                                          final TimeUnit unit,
                                          final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpdateLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryUpdateLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryUpdateLockNanos(final long address,
                                              final long timeInNanos,
                                              final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long end = System.nanoTime() + timeInNanos;
        do {
            if (LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            Jvm.nanoPause();
        } while (System.nanoTime() <= end);
        return false;
    }

    /**
     * Use a timer which is more insensitive to jumps in time like GCs and context switches.
     */
    private static boolean tryUpdateLockMillis(final long address,
                                               long timeInMillis,
                                               final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        long lastTime = System.currentTimeMillis();
        do {
            if (LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET))
                return true;
            checkInterrupted(interruptible);
            Jvm.nanoPause();
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeInMillis--;
            }
        } while (timeInMillis >= 0);
        return false;
    }

    private static boolean innerTryWriteLock(final long address,
                                             final long time,
                                             final TimeUnit unit,
                                             final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert address % 4 == 0;
        return LOCK.tryWriteLock(A, null, address + LOCK_OFFSET) ||
                tryWriteLock0(address, time, unit, interruptible);
    }

    private static boolean tryWriteLock0(final long address,
                                         final long time,
                                         final TimeUnit unit,
                                         final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryWriteLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryWriteLockMillis(address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryWriteLockNanos(final long address,
                                             final long timeInNanos,
                                             final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        try {
            do {
                if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                    return true;
                checkInterrupted(interruptible);
                Jvm.nanoPause();
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
    private static boolean tryWriteLockMillis(final long address,
                                              long timeInMillis,
                                              final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        try {
            do {
                if (LOCK.tryWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET))
                    return true;
                checkInterrupted(interruptible);
                Jvm.nanoPause();
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
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.registerWait(A, null, address + LOCK_OFFSET);
    }

    private static void deregisterWait(long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.deregisterWait(A, null, address + LOCK_OFFSET);
    }

    private static boolean innerTryUpgradeUpdateToWriteLock(final long address,
                                                            final long time,
                                                            final TimeUnit unit,
                                                            final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpgradeUpdateToWriteLock(A, null, address + LOCK_OFFSET) ||
                tryUpgradeUpdateToWriteLock0(address, time, unit, interruptible);
    }

    private static boolean tryUpgradeUpdateToWriteLock0(final long address,
                                                        final long time,
                                                        final TimeUnit unit,
                                                        final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long timeInNanos = unit.toNanos(time);
        if (timeInNanos < TRY_LOCK_NANOS_THRESHOLD) {
            return tryUpgradeUpdateToWriteLockNanos(address, timeInNanos, interruptible);
        } else {
            return tryUpgradeUpdateToWriteLockMillis(
                    address, roundUpNanosToMillis(timeInNanos), interruptible);
        }
    }

    private static boolean tryUpgradeUpdateToWriteLockNanos(final long address,
                                                            final long timeInNanos,
                                                            final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        final long end = System.nanoTime() + timeInNanos;
        registerWait(address);
        try {
            do {
                if (tryUpgradeUpdateToWriteLockAndDeregisterWait0(address))
                    return true;
                checkInterrupted(interruptible);
                Jvm.nanoPause();
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
    private static boolean tryUpgradeUpdateToWriteLockMillis(final long address,
                                                             long timeInMillis,
                                                             final boolean interruptible) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        long lastTime = System.currentTimeMillis();
        registerWait(address);
        try {
            do {
                if (tryUpgradeUpdateToWriteLockAndDeregisterWait0(address))
                    return true;
                checkInterrupted(interruptible);
                Jvm.nanoPause();
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

    private static RuntimeException tryDeregisterWaitAndRethrow(final long address,
                                                                final Throwable throwable) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        try {
            deregisterWait(address);
        } catch (Throwable t) {
            throwable.addSuppressed(t);
        }
        throw Jvm.rethrow(throwable);
    }

    private static boolean tryUpgradeUpdateToWriteLockAndDeregisterWait0(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpgradeUpdateToWriteLockAndDeregisterWait(A, null, address + LOCK_OFFSET);
    }

    @Override
    public long entries(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return OS.memory().readInt(address + ENTRIES_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void entries(final long address, final long entries) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (entries >= (1L << 32)) {
            throw new IllegalStateException("segment entries overflow: up to " + UNSIGNED_INT_MASK +
                    " supported, " + entries + " given");
        }
        OS.memory().writeInt(address + ENTRIES_OFFSET, (int) entries);
    }

    @Override
    public long deleted(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return OS.memory().readInt(address + DELETED_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void deleted(final long address,
                        final long deleted) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (deleted >= (1L << 32)) {
            throw new IllegalStateException("segment deleted entries count overflow: up to " +
                    UNSIGNED_INT_MASK + " supported, " + deleted + " given");
        }
        OS.memory().writeInt(address + DELETED_OFFSET, (int) deleted);
    }

    @Override
    public long lowestPossiblyFreeChunk(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return OS.memory().readInt(address + LOWEST_POSSIBLY_FREE_CHUNK_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void lowestPossiblyFreeChunk(final long address,
                                        final long lowestPossiblyFreeChunk) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        OS.memory().writeInt(address + LOWEST_POSSIBLY_FREE_CHUNK_OFFSET,
                (int) lowestPossiblyFreeChunk);
    }

    @Override
    public long nextTierIndex(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return OS.memory().readLong(address + NEXT_TIER_INDEX_OFFSET);
    }

    @Override
    public void nextTierIndex(final long address,
                              final long nextTierIndex) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        OS.memory().writeLong(address + NEXT_TIER_INDEX_OFFSET, nextTierIndex);
    }

    @Override
    public void readLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        try {
            if (!innerTryReadLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Override
    public void readLockInterruptibly(final long address) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (!tryReadLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryReadLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryReadLock(final long address,
                               final long time,
                               final TimeUnit unit) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return innerTryReadLock(address, time, unit, true);
    }

    @Override
    public boolean tryUpgradeReadToUpdateLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpgradeReadToUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpgradeReadToWriteLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpgradeReadToWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void updateLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        try {
            if (!innerTryUpdateLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Override
    public void updateLockInterruptibly(final long address) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (!tryUpdateLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryUpdateLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpdateLock(final long address,
                                 final long time,
                                 final TimeUnit unit) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return innerTryUpdateLock(address, time, unit, true);
    }

    @Override
    public void writeLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        try {
            if (!innerTryWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Override
    public void writeLockInterruptibly(final long address) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (!tryWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryWriteLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert address % 4 == 0;
        return LOCK.tryWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryWriteLock(final long address,
                                final long time,
                                final TimeUnit unit) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return innerTryWriteLock(address, time, unit, true);
    }

    @Override
    public void upgradeUpdateToWriteLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        try {
            if (!innerTryUpgradeUpdateToWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS, false))
                throw deadLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Override
    public void upgradeUpdateToWriteLockInterruptibly(final long address) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        if (!tryUpgradeUpdateToWriteLock(address, LOCK_TIMEOUT_SECONDS, SECONDS))
            throw deadLock();
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.tryUpgradeUpdateToWriteLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(final long address,
                                               final long time,
                                               final TimeUnit unit) throws InterruptedException {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return innerTryUpgradeUpdateToWriteLock(address, time, unit, true);
    }

    @Override
    public void readUnlock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.readUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void updateUnlock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.updateUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeUpdateToReadLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.downgradeUpdateToReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void writeUnlock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.writeUnlock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeWriteToUpdateLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.downgradeWriteToUpdateLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void downgradeWriteToReadLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.downgradeWriteToReadLock(A, null, address + LOCK_OFFSET);
    }

    @Override
    public void resetLock(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        LOCK.reset(A, null, address + LOCK_OFFSET);
    }

    @Override
    public long resetLockState() {
        return LOCK.resetState();
    }

    @Override
    public long getLockState(final long address) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        return LOCK.getState(A, null, address + LOCK_OFFSET);
    }

    @Override
    public String lockStateToString(long lockState) {
        return LOCK.toString(lockState);
    }
}
