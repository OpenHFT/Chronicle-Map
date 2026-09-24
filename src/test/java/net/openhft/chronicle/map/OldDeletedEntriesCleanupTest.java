/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.SegmentLock;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Regression test for Chronicle-Map#310.
 *
 * <p>A removed entry in a replicated map leaves a tombstone (an absent replicable entry) so the
 * deletion can propagate. Those tombstones are reclaimed in the background by
 * {@link OldDeletedEntriesCleanupThread} once {@code removedEntryCleanupTimeout} has elapsed - this
 * must happen on its own, without any later {@code put()}/query touching the segment.
 *
 * <p>Before the fix, {@code run()} exited the cleanup thread outright whenever less than a second
 * had elapsed since construction. Because the thread is started immediately after construction, it
 * almost always terminated before removing a single entry, so tombstones were only ever reclaimed
 * as a side effect of a later operation on the same slot. This test creates tombstones, then waits
 * (doing nothing else to the map) for the background thread to clear them.
 */
public class OldDeletedEntriesCleanupTest {

    @Test(timeout = 30_000)
    public void tombstonesAreClearedByBackgroundCleanupWithoutLaterPut() {
        final ChronicleMapBuilder<Integer, CharSequence> builder = replicatedBuilder(
                1, TimeUnit.MILLISECONDS);

        try (ChronicleMap<Integer, CharSequence> map = builder.create()) {
            for (int i = 1; i <= 5; i++)
                map.put(i, "value-" + i);
            for (int i = 1; i <= 5; i++)
                map.remove(i);

            assertEquals(0, map.size());
            // The removals leave tombstones behind - replicable entries still occupy the segment.
            assertTrue("expected tombstones to be present right after removal",
                    countReplicableEntries(map) > 0);

            // Wait for the background cleanup thread to erase the tombstones. Crucially we perform
            // NO further map operations here: the entries must be reclaimed by the thread itself.
            final long deadline = System.currentTimeMillis() + 25_000;
            while (countReplicableEntries(map) > 0 && System.currentTimeMillis() < deadline)
                Jvm.pause(50);

            assertEquals("background cleanup should have erased all tombstones without a later put",
                    0, countReplicableEntries(map));
        }
    }

    /**
     * A microsecond-based timeout exercises the cleaner's sub-millisecond wait path. Two tombstone
     * batches are created on opposite sides of a completed cleanup pass so the second batch can
     * only disappear if the cleaner wakes and performs another cycle.
     */
    @Test(timeout = 30_000)
    public void microsecondTimeoutCleansAcrossMultipleCycles() {
        try (ChronicleMap<Integer, CharSequence> map = replicatedBuilder(
                1_500, TimeUnit.MICROSECONDS).create()) {
            removeAndAwaitCleanup(map, 1);
            removeAndAwaitCleanup(map, 2);
        }
    }

    /**
     * {@link java.util.concurrent.locks.LockSupport#parkNanos(Object, long)} accepts a relative
     * duration. Passing an absolute {@link System#nanoTime()} deadline parks effectively forever.
     */
    @Test(timeout = 10_000)
    public void nanosecondSleepUsesRelativeDuration() throws InterruptedException {
        try (ChronicleMap<Integer, CharSequence> map = replicatedBuilder(
                1_500, TimeUnit.MICROSECONDS).create()) {
            final OldDeletedEntriesCleanupThread cleaner = cleanupThread(map);
            final Thread sleeper = new Thread(
                    () -> cleaner.sleepNanos(TimeUnit.MICROSECONDS.toNanos(500)),
                    "cleanup-nanos-regression");
            sleeper.start();
            sleeper.join(2_000);
            try {
                assertFalse("a 0.5 ms relative wait must not remain parked for seconds",
                        sleeper.isAlive());
            } finally {
                sleeper.interrupt();
                sleeper.join(2_000);
            }
        }
    }

    /**
     * Closing a replicated map must stop the cleaner before the base close path releases its
     * registered iteration context. This test holds the cleaner's segment lock, starts close, and
     * interrupts the closing thread while it joins. Releasing the lock must let both cleaner and
     * close finish, with the closing thread's interrupt status restored.
     */
    @Test(timeout = 30_000)
    public void closeWaitsForCleanerAndRestoresInterruptStatus() throws InterruptedException {
        final ChronicleMap<Integer, CharSequence> map = replicatedBuilder(
                1, TimeUnit.MILLISECONDS).create();
        final MapSegmentContext<Integer, CharSequence, ?> context = map.segmentContext(0);
        final SegmentLock segmentLock = (SegmentLock) context;
        final OldDeletedEntriesCleanupThread cleaner = cleanupThread(map);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicBoolean interruptRestored = new AtomicBoolean();
        final Thread closer = new Thread(() -> {
            try {
                map.close();
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable t) {
                closeFailure.set(t);
            }
        }, "map-close-regression");

        segmentLock.updateLock().lock();
        try {
            awaitSegmentLockAcquisition(cleaner);

            closer.start();
            final long shutdownDeadline = System.currentTimeMillis() + 5_000;
            while (!closer.isAlive() && System.currentTimeMillis() < shutdownDeadline)
                Jvm.pause(10);
            closer.interrupt();
            Jvm.pause(100);

            assertTrue("close must wait while the cleaner owns or awaits its segment context",
                    closer.isAlive());
            assertTrue("cleaner must still be alive while its segment lock is held",
                    cleaner.isAlive());
        } finally {
            segmentLock.updateLock().unlock();
            context.close();
        }

        closer.join(5_000);
        assertFalse("map.close() must complete within its bounded cleaner join", closer.isAlive());
        assertFalse("cleaner must exit before map.close() returns", cleaner.isAlive());
        assertNull("map.close() should not fail once the cleaner's lock is released",
                closeFailure.get());
        assertTrue("interrupt status must be restored after the join completes",
                interruptRestored.get());
        assertTrue(map.isClosed());
    }

    @Test(timeout = 30_000)
    public void closeFailsWithinConfiguredDeadlineWhenCleanerCannotLeaveSegmentLockWait()
            throws InterruptedException {
        final ChronicleMapBuilder<Integer, CharSequence> builder = replicatedBuilder(
                1, TimeUnit.MILLISECONDS);
        final ChronicleHashBuilderPrivateAPI<?, ?> privateAPI =
                Objects.requireNonNull(Jvm.getValue(builder, "privateAPI"));
        privateAPI.cleanupRemovedEntries(false);

        final ChronicleMap<Integer, CharSequence> map = builder.create();
        final MapSegmentContext<Integer, CharSequence, ?> context = map.segmentContext(0);
        final SegmentLock segmentLock = (SegmentLock) context;
        final OldDeletedEntriesCleanupThread cleaner = new OldDeletedEntriesCleanupThread(
                (ReplicatedChronicleMap<?, ?, ?>) map, 100, TimeUnit.MILLISECONDS);

        segmentLock.updateLock().lock();
        try {
            cleaner.start();
            awaitSegmentLockAcquisition(cleaner);
            final long start = System.nanoTime();
            try {
                cleaner.close();
                fail("close should fail rather than release resources under a live cleaner");
            } catch (IllegalStateException expected) {
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - start);
                assertTrue("configured close deadline should be observed", elapsedMillis >= 75);
                assertTrue("close deadline must remain bounded", elapsedMillis < 2_000);
                assertTrue(expected.getMessage().contains("resources have not been released"));
                assertTrue("cleaner is deliberately still alive at timeout", cleaner.isAlive());
            }
        } finally {
            segmentLock.updateLock().unlock();
            context.close();
        }

        cleaner.join(5_000);
        assertFalse("cleaner must exit after the segment lock is released", cleaner.isAlive());
        map.close();
    }

    private static ChronicleMapBuilder<Integer, CharSequence> replicatedBuilder(
            long cleanupTimeout, TimeUnit unit) {
        final ChronicleMapBuilder<Integer, CharSequence> builder = ChronicleMap
                .of(Integer.class, CharSequence.class)
                .entries(1000)
                .averageValueSize(20)
                .actualSegments(1);

        final ChronicleHashBuilderPrivateAPI<?, ?> privateAPI =
                Objects.requireNonNull(Jvm.getValue(builder, "privateAPI"));
        privateAPI.replication((byte) 1);
        privateAPI.removedEntryCleanupTimeout(cleanupTimeout, unit);
        return builder;
    }

    private static void removeAndAwaitCleanup(
            ChronicleMap<Integer, CharSequence> map, int key) {
        map.put(key, "value-" + key);
        map.remove(key);
        assertTrue("expected a tombstone immediately after removal",
                countReplicableEntries(map) > 0);

        final long deadline = System.currentTimeMillis() + 10_000;
        while (countReplicableEntries(map) > 0 && System.currentTimeMillis() < deadline)
            Jvm.pause(10);
        assertEquals("background cleanup should complete this tombstone batch",
                0, countReplicableEntries(map));
    }

    private static OldDeletedEntriesCleanupThread cleanupThread(
            ChronicleMap<Integer, CharSequence> map) {
        return Objects.requireNonNull(Jvm.getValue(map, "oldDeletedEntriesCleanupThread"));
    }

    private static void awaitSegmentLockAcquisition(Thread cleaner) {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!isAcquiringSegmentLock(cleaner) && System.currentTimeMillis() < deadline)
            Jvm.pause(10);
        assertTrue("cleaner must reach segment-lock acquisition, not merely its startup sleep",
                isAcquiringSegmentLock(cleaner));
    }

    private static boolean isAcquiringSegmentLock(Thread cleaner) {
        for (StackTraceElement frame : cleaner.getStackTrace()) {
            if (frame.getClassName().endsWith("UpdateLock") &&
                    frame.getMethodName().equals("lock"))
                return true;
        }
        return false;
    }

    private static int countReplicableEntries(ChronicleMap<Integer, CharSequence> map) {
        final int[] count = {0};
        for (int i = 0; i < map.segments(); i++) {
            try (MapSegmentContext<Integer, CharSequence, ?> context = map.segmentContext(i)) {
                ((ReplicatedHashSegmentContext<Integer, ?>) context)
                        .forEachSegmentReplicableEntry(e -> count[0]++);
            }
        }
        return count[0];
    }
}
