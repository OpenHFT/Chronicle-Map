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

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;
import static net.openhft.chronicle.hash.replication.TimeProvider.systemTimeIntervalBetween;

class OldDeletedEntriesCleanupThread extends Thread
        implements Closeable, Predicate<ReplicableEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(OldDeletedEntriesCleanupThread.class);

    /**
     * Don't store a strong ref to a map in order to avoid it's leaking, if the user forgets to
     * close() map, from where this thread is shut down explicitly. Dereference map within
     * a single method, {@link #cleanupSegment()}. The map has a chance to be collected by GC when
     * this thread is sleeping after cleaning up a segment.
     */
    private final WeakReference<ReplicatedChronicleMap<?, ?, ?>> mapRef;
    /**
     * {@code cleanupTimeout}, {@link #cleanupTimeoutUnit} and {@link #segments} are parts of the
     * cleaned Map's state, extracted in order to minimize accesses to the map.
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
     * This object is used to determine that this thread is parked from {@link #sleepMillis(long)}
     * or {@link #sleepNanos(long)}, not somewhere inside ChronicleMap logic, to interrupt()
     * selectively in {@link #close()}.
     */
    private final Object cleanupSleepingHandle = new Object();

    private volatile boolean shutdown;

    private long prevSegment0ScanStart = -1;
    private long removedCompletely;

    OldDeletedEntriesCleanupThread(ReplicatedChronicleMap<?, ?, ?> map) {
        super("Cleanup Thread for " + map.toIdentityString());
        setDaemon(true);
        this.mapRef = new WeakReference<>(map);
        cleanupTimeout = map.cleanupTimeout;
        cleanupTimeoutUnit = map.cleanupTimeoutUnit;
        segments = map.segments();

        segmentsPermutation = randomPermutation(map.segments());
        inverseSegmentsPermutation = inversePermutation(segmentsPermutation);
    }

    private static int[] randomPermutation(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = i;
        }
        shuffle(a);
        return a;
    }

    // Implementing Fisher–Yates shuffle
    private static void shuffle(int[] a) {
        Random rnd = ThreadLocalRandom.current();
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
        while (!shutdown) {
            int nextSegmentIndex = cleanupSegment();
            if (nextSegmentIndex == -1)
                return;
            if (nextSegmentIndex == 0) {
                long currentTime = currentTime();
                long mapScanTime = systemTimeIntervalBetween(
                        prevSegment0ScanStart, currentTime, cleanupTimeoutUnit);
                LOG.debug("Old deleted entries scan time: {} {}", mapScanTime, cleanupTimeoutUnit);
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
                LOG.debug("Removed {} old deleted entries in the segment {}",
                        removedCompletely, segmentIndex);
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

    private void sleepNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline && !shutdown)
            LockSupport.parkNanos(cleanupSleepingHandle, deadline);
    }

    @Override
    public void close() {
        shutdown = true;
        // this means blocked in sleepMillis() or sleepNanos()
        if (LockSupport.getBlocker(this) == cleanupSleepingHandle)
            this.interrupt(); // unblock
    }

    private int nextSegmentIndex(int segmentIndex) {
        int permutationIndex = inverseSegmentsPermutation[segmentIndex];
        int nextPermutationIndex = (permutationIndex + 1) % segments;
        return segmentsPermutation[nextPermutationIndex];
    }
}
