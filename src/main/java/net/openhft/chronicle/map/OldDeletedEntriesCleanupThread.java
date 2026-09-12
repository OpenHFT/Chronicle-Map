/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.impl.BigSegmentHeader;
import net.openhft.chronicle.hash.replication.ReplicableEntry;

import java.lang.ref.WeakReference;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;
import static net.openhft.chronicle.hash.replication.TimeProvider.systemTimeIntervalBetween;

class OldDeletedEntriesCleanupThread extends Thread
        implements MapClosable, Predicate<ReplicableEntry> {

    /**
     * Don't store a strong ref to a map in order to avoid it's leaking, if the user forgets to close() map, from where this thread is shut down
     * explicitly. Dereference map within a single method, {@link #cleanupSegment()}. The map has a chance to be collected by GC when this thread is
     * sleeping after cleaning up a segment.
     */
    private final WeakReference<ReplicatedChronicleMap<?, ?, ?>> mapRef;
    /**
     * {@code cleanupTimeout}, {@link #cleanupTimeoutUnit} and {@link #segments} are parts of the
     * cleaned Map's state, extracted in order to minimise accesses to the map.
     *
     * @see ChronicleHashBuilderPrivateAPI#removedEntryCleanupTimeout(long, TimeUnit)
     */
    private final long cleanupTimeout;
    private final TimeUnit cleanupTimeoutUnit;
    private final int segments;

    /**
     * {@code segmentsPermutation} and {@link #inverseSegmentsPermutation} determine random order,
     * in which segments are cleaned up.
     */
    private final int[] segmentsPermutation;
    private final int[] inverseSegmentsPermutation;

    /**
     * Identifies parks performed by {@link #sleepMillis(long)} and {@link #sleepNanos(long)} in
     * thread dumps and diagnostics.
     */
    private final Object cleanupSleepingHandle = new Object();
    private final long closeJoinTimeoutNanos;

    private volatile boolean shutdown;

    private long prevSegment0ScanStart = -1;
    private long removedCompletely;
    private final long startTime = System.currentTimeMillis();

    OldDeletedEntriesCleanupThread(ReplicatedChronicleMap<?, ?, ?> map) {
        this(map, defaultCloseJoinTimeoutNanos(), TimeUnit.NANOSECONDS);
    }

    OldDeletedEntriesCleanupThread(ReplicatedChronicleMap<?, ?, ?> map,
                                   long closeJoinTimeout,
                                   TimeUnit closeJoinTimeoutUnit) {
        super("Cleanup Thread for " + map.toIdentityString());
        if (closeJoinTimeout <= 0)
            throw new IllegalArgumentException("closeJoinTimeout must be positive");
        setDaemon(true);
        this.mapRef = new WeakReference<>(map);
        this.closeJoinTimeoutNanos = closeJoinTimeoutUnit.toNanos(closeJoinTimeout);
        cleanupTimeout = map.cleanupTimeout;
        cleanupTimeoutUnit = map.cleanupTimeoutUnit;
        segments = map.segments();

        segmentsPermutation = randomPermutation(map.segments());
        inverseSegmentsPermutation = inversePermutation(segmentsPermutation);
    }

    private static long defaultCloseJoinTimeoutNanos() {
        // A cleaner already waiting for a segment lock uses this timeout. Give that acquisition a
        // chance to report its own dead-lock failure, then stop waiting rather than hanging close.
        final long lockTimeoutSeconds = Math.max(0L, BigSegmentHeader.LOCK_TIMEOUT_SECONDS);
        return TimeUnit.SECONDS.toNanos(lockTimeoutSeconds + 1L);
    }

    private static int[] randomPermutation(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = i;
        }
        shuffle(a);
        return a;
    }

    // Implementing Fisher-Yates shuffle
    private static void shuffle(int[] a) {
        SecureRandom rnd = new SecureRandom();
        for (int i = a.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            int e = a[index];
            a[index] = a[i];
            a[i] = e;
        }
    }

    private static int[] inversePermutation(int[] permutation) {
        int n = permutation.length;
        int[] inverse = new int[n];
        for (int i = 0; i < n; i++) {
            inverse[permutation[i]] = i;
        }
        return inverse;
    }

    @Override
    public void run() {
        throwExceptionIfClosed();

        // Delay the first cleanup pass by up to a second after construction (see "delayed cleaner").
        // Historically this exited the thread outright when less than a second had elapsed, which -
        // because the thread is started immediately after construction - meant the cleanup thread
        // almost always terminated before removing a single old deleted entry, so tombstones were
        // only ever reclaimed as a side effect of a later put()/replication event on the segment.
        // Wait out the remaining delay instead, respecting shutdown, then run the cleanup loop.
        long remainingStartupDelay = 1_000 - (System.currentTimeMillis() - startTime);
        if (remainingStartupDelay > 0)
            sleepMillis(remainingStartupDelay);

        while (!shutdown) {
            int nextSegmentIndex;
            try {
                nextSegmentIndex = cleanupSegment();
            } catch (Exception e) {
                if (shutdown)
                    break;
                throw e;
            }
            if (nextSegmentIndex == -1)
                return;
            if (nextSegmentIndex == 0) {
                long currentTime = currentTime();
                long mapScanTime = systemTimeIntervalBetween(
                        prevSegment0ScanStart, currentTime, cleanupTimeoutUnit);
                Jvm.debug().on(getClass(), "Old deleted entries scan time: " + mapScanTime + " " + cleanupTimeoutUnit);
                if (mapScanTime < cleanupTimeout) {
                    long timeToSleep = cleanupTimeoutUnit.toMillis(cleanupTimeout - mapScanTime);
                    if (timeToSleep > 0) {
                        sleepMillis(timeToSleep);
                    } else {
                        sleepNanos(cleanupTimeoutUnit.toNanos(cleanupTimeout - mapScanTime));
                    }
                }
            }
        }
    }

    /**
     * @return next segment index to cleanup, or -1 if cleanup thread should be shut down
     */
    private int cleanupSegment() {
        ReplicatedChronicleMap<?, ?, ?> map = mapRef.get();
        if (map == null)
            return -1;
        int segmentIndex = map.globalMutableState().getCurrentCleanupSegmentIndex();
        int nextSegmentIndex;
        try (MapSegmentContext<?, ?, ?> context = map.segmentContext(segmentIndex)) {
            if (segmentIndex == 0)
                prevSegment0ScanStart = currentTime();
            removedCompletely = 0;
            if (((ReplicatedHashSegmentContext<?, ?>) context)
                    .forEachSegmentReplicableEntryWhile(this)) {
                Jvm.debug().on(getClass(),
                        "Removed " + removedCompletely + " old deleted entries " +
                                "in the segment " + segmentIndex);
                nextSegmentIndex = nextSegmentIndex(segmentIndex);
                map.globalMutableState().setCurrentCleanupSegmentIndex(nextSegmentIndex);
                return nextSegmentIndex;
            } else {
                // forEachWhile returned false => interrupted => shutdown = true
                assert shutdown;
                return -1;
            }
        }
    }

    @Override
    public boolean test(ReplicableEntry e) {
        throwExceptionIfClosed();

        if (shutdown)
            return false;
        if (e instanceof MapAbsentEntry) {
            long deleteTimeout = systemTimeIntervalBetween(
                    e.originTimestamp(), currentTime(), cleanupTimeoutUnit);
            if (deleteTimeout > cleanupTimeout && !e.isChanged()) {
                e.doRemoveCompletely();
                removedCompletely++;
            }
        }
        return true;
    }

    private void sleepMillis(long millis) {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline && !shutdown)
            LockSupport.parkUntil(cleanupSleepingHandle, deadline);
    }

    void sleepNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0 && !shutdown)
            LockSupport.parkNanos(cleanupSleepingHandle, remaining);
    }

    @Override
    public void close() {
        shutdown = true;
        // Unblock both our explicit sleeps and a segment-lock wait. Map resources, including the
        // thread's iteration context, must not be released until run() has finished.
        interrupt();
        if (Thread.currentThread() == this)
            return;

        boolean interrupted = false;
        final long deadline = System.nanoTime() + closeJoinTimeoutNanos;
        while (isAlive()) {
            final long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                if (interrupted)
                    Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Cleanup thread did not stop within " +
                                TimeUnit.NANOSECONDS.toMillis(closeJoinTimeoutNanos) +
                                " milliseconds; Chronicle Map resources have not been released");
            }
            try {
                TimeUnit.NANOSECONDS.timedJoin(this, remaining);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private int nextSegmentIndex(int segmentIndex) {
        int permutationIndex = inverseSegmentsPermutation[segmentIndex];
        int nextPermutationIndex = (permutationIndex + 1) % segments;
        return segmentsPermutation[nextPermutationIndex];
    }
}
