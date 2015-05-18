/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.hash.locks.IllegalInterProcessLockStateException;

import java.util.concurrent.TimeUnit;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;
import static net.openhft.lang.io.AbstractBytes.UNSIGNED_INT_MASK;
import static net.openhft.lang.io.NativeBytes.UNSAFE;

public final class BigSegmentHeader implements SegmentHeader {
    public static final BigSegmentHeader INSTANCE = new BigSegmentHeader();

    static final long LOCK_OFFSET = 0L; // 64-bit
    static final long COUNT_WORD_OFFSET = LOCK_OFFSET;
    static final long WAIT_WORD_OFFSET = LOCK_OFFSET + 4L;

    static final int COUNT_WORD_SHIFT = nativeOrder() == LITTLE_ENDIAN ? 0 : 32;
    static final int WAIT_WORD_SHIFT = 32 - COUNT_WORD_SHIFT;

    static final int READ_BITS = 30;
    static final int MAX_READ = (1 << READ_BITS) - 1;
    static final int READ_MASK = MAX_READ;
    static final int READ_PARTY = 1;

    static final int UPDATE_BIT = 1 << READ_BITS;
    static final int UPDATE_PARTY = READ_PARTY | UPDATE_BIT;
    static final int WRITE_BIT = UPDATE_BIT << 1;
    static final int WRITE_LOCKED_COUNT_WORD = UPDATE_PARTY | WRITE_BIT;

    static final int MAX_WAIT = Integer.MAX_VALUE;
    static final int WAIT_PARTY = 1;

    static final long SIZE_OFFSET = LOCK_OFFSET + 8L; // 32-bit
    static final long NEXT_POS_TO_SEARCH_FROM_OFFSET = SIZE_OFFSET + 4L;


    static final long EXCLUSIVE_LOCK_HOLDER_THREAD_ID_OFFSET = NEXT_POS_TO_SEARCH_FROM_OFFSET + 4L;

    static final long DELETED_OFFSET = EXCLUSIVE_LOCK_HOLDER_THREAD_ID_OFFSET + 8L;

    private BigSegmentHeader() {
    }

    @Override
    public long size(long address) {
        return UNSAFE.getInt(address + SIZE_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void size(long address, long size) {
        UNSAFE.putInt(address + SIZE_OFFSET, (int) size);
    }

    @Override
    public long deleted(long address) {
        return UNSAFE.getInt(address + DELETED_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void deleted(long address, long deleted) {
        UNSAFE.putInt(address + DELETED_OFFSET, (int) deleted);
    }

    @Override
    public long nextPosToSearchFrom(long address) {
        return UNSAFE.getInt(address + NEXT_POS_TO_SEARCH_FROM_OFFSET) & UNSIGNED_INT_MASK;
    }

    @Override
    public void nextPosToSearchFrom(long address, long nextPosToSearchFrom) {
        UNSAFE.putInt(address + NEXT_POS_TO_SEARCH_FROM_OFFSET, (int) nextPosToSearchFrom);
    }

    private static long getLockWord(long address) {
        return UNSAFE.getLongVolatile(null, address + LOCK_OFFSET);
    }

    private static boolean casLockWord(long address, long expected, long x) {
        return UNSAFE.compareAndSwapLong(null, address + LOCK_OFFSET, expected, x);
    }

    private static int countWord(long lockWord) {
        return (int) (lockWord >> COUNT_WORD_SHIFT);
    }

    private static int waitWord(long lockWord) {
        return (int) (lockWord >> WAIT_WORD_SHIFT);
    }

    private static long lockWord(int countWord, int waitWord) {
        return ((((long) countWord) & UNSIGNED_INT_MASK) << COUNT_WORD_SHIFT) |
                ((((long) waitWord) & UNSIGNED_INT_MASK) << WAIT_WORD_SHIFT);
    }

    private static int getCountWord(long address) {
        return UNSAFE.getIntVolatile(null, address + COUNT_WORD_OFFSET);
    }

    private static boolean casCountWord(long address, int expected, int x) {
        return UNSAFE.compareAndSwapInt(null, address + COUNT_WORD_OFFSET, expected, x);
    }

    private static void putCountWord(long address, int countWord) {
        UNSAFE.putOrderedInt(null, address + COUNT_WORD_OFFSET, countWord);
    }

    private static boolean writeLocked(int countWord) {
        return countWord == WRITE_LOCKED_COUNT_WORD;
    }

    private static void checkWriteLocked(int countWord) {
        if (countWord != WRITE_LOCKED_COUNT_WORD)
            throw new IllegalInterProcessLockStateException("Expected write lock");
    }

    private static boolean updateLocked(int countWord) {
        return (countWord & UPDATE_BIT) != 0;
    }

    private static void checkUpdateLocked(int countWord) {
        if (countWord < UPDATE_PARTY) // i. e. if update bit is not set, or write bit is set
            throw new IllegalInterProcessLockStateException("Expected update lock");
    }

    private static int readCount(int countWord) {
        return countWord & READ_MASK;
    }

    private static void checkReadLocked(int countWord) {
        if (countWord <= 0) // i. e. if read count == 0 or write bit (actually sign bit) is set
            throw new IllegalInterProcessLockStateException("Expected read lock");
    }

    private static void checkReadCountForIncrement(int countWord) {
        if (readCount(countWord) == MAX_READ) {
            throw new IllegalInterProcessLockStateException(
                    "Lock count reached the limit of " + MAX_READ);
        }
    }

    private static int getWaitWord(long address) {
        return UNSAFE.getIntVolatile(null, address + WAIT_WORD_OFFSET);
    }

    private static boolean casWaitWord(long address, int expected, int x) {
        return UNSAFE.compareAndSwapInt(null, address + WAIT_WORD_OFFSET, expected, x);
    }

    private static void checkWaitWordForIncrement(int waitWord) {
        if (waitWord == MAX_WAIT) {
            throw new IllegalInterProcessLockStateException(
                    "Wait count reached the limit of " + MAX_WAIT);
        }
    }

    private static void checkWaitWordForDecrement(int waitWord) {
        if (waitWord == 0) {
            throw new IllegalInterProcessLockStateException(
                    "Wait count underflowed");
        }
    }

    private static void writeExclusiveLockHolder(long address) {
        UNSAFE.putLong(address + EXCLUSIVE_LOCK_HOLDER_THREAD_ID_OFFSET,
                Thread.currentThread().getId());
    }

    private static void clearExclusiveLockHolder(long address) {
        UNSAFE.putLong(address + EXCLUSIVE_LOCK_HOLDER_THREAD_ID_OFFSET, 0L);
    }

    /**
     * For debugging and monitoring
     */
    static Thread exclusiveLockHolder(long address) {
        long holderId = UNSAFE.getLong(address + EXCLUSIVE_LOCK_HOLDER_THREAD_ID_OFFSET);
        if (holderId == 0L)
            return null;
        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (Thread thread : threads) {
            if (thread.getId() == holderId)
                return thread;
        }
        return null;
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
        long lockWord = getLockWord(address);
        int countWord = countWord(lockWord);
        if (!writeLocked(countWord) && waitWord(lockWord) == 0) {
            checkReadCountForIncrement(countWord);
            if (casCountWord(address, countWord, countWord + READ_PARTY))
                return true;
        }
        return false;
    }

    @Override
    public boolean tryReadLock(long address, long time, TimeUnit unit) {
        return tryReadLock(address) || tryReadLock0(address, time, unit);
    }

    private boolean tryReadLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < 2000000) {
            return tryReadLockNanos(address, timeInNanos);
        } else {
            return tryReadLockMillis(address, (timeInNanos + 900000) / 1000000);
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
        int countWord = getCountWord(address);
        checkReadLocked(countWord);
        return !updateLocked(countWord) &&
                casCountWord(address, countWord, countWord - READ_PARTY + UPDATE_PARTY);
    }

    @Override
    public boolean tryUpgradeReadToWriteLock(long address) {
        int countWord = getCountWord(address);
        checkReadLocked(countWord);
        return countWord == READ_PARTY &&
                casCountWord(address, READ_PARTY, WRITE_LOCKED_COUNT_WORD);
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
        long lockWord = getLockWord(address);
        int countWord = countWord(lockWord);
        if (!updateLocked(countWord) && waitWord(lockWord) == 0) {
            checkReadCountForIncrement(countWord);
            if (casCountWord(address, countWord, countWord + UPDATE_PARTY)) {
                writeExclusiveLockHolder(address);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean tryUpdateLock(long address, long time, TimeUnit unit) {
        return tryUpdateLock(address) || tryUpdateLock0(address, time, unit);
    }

    private boolean tryUpdateLock0(long address, long time, TimeUnit unit) {
        long timeInNanos = unit.toNanos(time);
        if (timeInNanos < 2000000) {
            return tryUpdateLockNanos(address, timeInNanos);
        } else {
            return tryUpdateLockMillis(address, (timeInNanos + 900000) / 1000000);
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
        if (getCountWord(address) == 0 && casCountWord(address, 0, WRITE_LOCKED_COUNT_WORD)) {
            writeExclusiveLockHolder(address);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean tryWriteLock(long address, long time, TimeUnit unit) {
        return tryWriteLock(address) || tryWriteLock0(address, time, unit);
    }

    private boolean tryWriteLock0(long address, long time, TimeUnit unit) {
        long end = System.nanoTime() + unit.toNanos(time);
        registerWait(address);
        do {
            long lockWord = getLockWord(address);
            int countWord = countWord(lockWord);
            if (countWord == 0) {
                int waitWord = waitWord(lockWord);
                checkWaitWordForDecrement(waitWord);
                if (casLockWord(address, lockWord,
                        lockWord(WRITE_LOCKED_COUNT_WORD, waitWord - WAIT_PARTY))) {
                    writeExclusiveLockHolder(address);
                    return true;
                }
            }
        } while (System.nanoTime() <= end);
        deregisterWait(address);
        return false;
    }

    private static void registerWait(long address) {
        while (true) {
            int waitWord = getWaitWord(address);
            checkWaitWordForIncrement(waitWord);
            if (casWaitWord(address, waitWord, waitWord + WAIT_PARTY))
                return;
        }
    }

    private static void deregisterWait(long address) {
        while (true) {
            int waitWord = getWaitWord(address);
            checkWaitWordForDecrement(waitWord);
            if (casWaitWord(address, waitWord, waitWord - WAIT_PARTY))
                return;
        }
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
        int countWord = getCountWord(address);
        return checkExclusiveUpdateLocked(countWord) &&
                casCountWord(address, countWord, WRITE_LOCKED_COUNT_WORD);
    }

    private static boolean checkExclusiveUpdateLocked(int countWord) {
        checkUpdateLocked(countWord);
        return countWord == UPDATE_PARTY;
    }

    @Override
    public boolean tryUpgradeUpdateToWriteLock(long address, long time, TimeUnit unit) {
        return tryUpgradeUpdateToWriteLock(address) ||
                tryUpgradeUpdateToWriteLock0(address, time, unit);
    }

    private boolean tryUpgradeUpdateToWriteLock0(long address, long time, TimeUnit unit) {
        long end = System.nanoTime() + unit.toNanos(time);
        registerWait(address);
        do {
            long lockWord = getLockWord(address);
            int countWord = countWord(lockWord);
            if (checkExclusiveUpdateLocked(countWord)) {
                int waitWord = waitWord(lockWord);
                checkWaitWordForDecrement(waitWord);
                if (casLockWord(address, lockWord,
                        lockWord(WRITE_LOCKED_COUNT_WORD, waitWord - WAIT_PARTY))) {
                    return true;
                }
            }
        } while (System.nanoTime() <= end);
        deregisterWait(address);
        return false;
    }

    @Override
    public void readUnlock(long address) {
        while (true) {
            int countWord = getCountWord(address);
            checkReadLocked(countWord);
            if (casCountWord(address, countWord, countWord - READ_PARTY))
                return;
        }
    }

    @Override
    public void updateUnlock(long address) {
        while (true) {
            int countWord = getCountWord(address);
            checkUpdateLocked(countWord);
            if (casCountWord(address, countWord, countWord - UPDATE_PARTY)) {
                clearExclusiveLockHolder(address);
                return;
            }
        }
    }

    @Override
    public void downgradeUpdateToReadLock(long address) {
        while (true) {
            int countWord = getCountWord(address);
            checkUpdateLocked(countWord);
            if (casCountWord(address, countWord, countWord ^ UPDATE_BIT)) {
                clearExclusiveLockHolder(address);
                return;
            }
        }
    }

    @Override
    public void writeUnlock(long address) {
        checkWriteLocked(getCountWord(address));
        clearExclusiveLockHolder(address);
        putCountWord(address, 0);
    }

    @Override
    public void downgradeWriteToUpdateLock(long address) {
        checkWriteLocked(getCountWord(address));
        putCountWord(address, UPDATE_PARTY);
    }

    @Override
    public void downgradeWriteToReadLock(long address) {
        checkWriteLocked(getCountWord(address));
        clearExclusiveLockHolder(address);
        putCountWord(address, READ_PARTY);
    }
}
