/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;
import static net.openhft.chronicle.hash.replication.TimeProvider.systemTimeIntervalBetween;

class OldDeletedEntriesCleanup implements Runnable, Closeable, Predicate<ReplicableEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(OldDeletedEntriesCleanup.class);

    private final ReplicatedChronicleMap<?, ?, ?> map;
    private final int[] segmentsPermutation;
    private final int[] inverseSegmentsPermutation;
    private volatile boolean shutdown;
    private volatile Thread runnerThread;
    private long prevSegment0ScanStart = -1;
    private long removedCompletely;

    public OldDeletedEntriesCleanup(ReplicatedChronicleMap<?, ?, ?> map) {
        this.map = map;
        segmentsPermutation = randomPermutation(map.segments());
        inverseSegmentsPermutation = inversePermutation(segmentsPermutation);
    }

    @Override
    public void run() {
        runnerThread = Thread.currentThread();
        while (!shutdown) {
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
                } else {
                    // forEachWhile returned false => interrupted => shutdown = true
                    assert shutdown;
                    return;
                }
            }
            if (nextSegmentIndex == 0) {
                long currentTime = currentTime();
                TimeUnit cleanupTimeoutUnit = map.cleanupTimeoutUnit;
                long mapScanTime = systemTimeIntervalBetween(
                        prevSegment0ScanStart, currentTime, cleanupTimeoutUnit);
                LOG.debug("Old deleted entries scan time: {} {}", mapScanTime, cleanupTimeoutUnit);
                long cleanupTimeout = map.cleanupTimeout;
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

    @Override
    public boolean test(ReplicableEntry e) {
        if (shutdown)
            return false;
        if (e instanceof MapAbsentEntry) {
            long deleteTimeout = systemTimeIntervalBetween(
                    e.originTimestamp(), currentTime(), map.cleanupTimeoutUnit);
            if (deleteTimeout > map.cleanupTimeout && !e.isChanged()) {
                e.doRemoveCompletely();
                removedCompletely++;
            }
        }
        return true;
    }

    private void sleepMillis(long millis) {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline && !shutdown)
            LockSupport.parkUntil(this, deadline);
    }

    private void sleepNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline && !shutdown)
            LockSupport.parkNanos(this, deadline);
    }

    @Override
    public void close() {
        shutdown = true;
        if (runnerThread != null &&
                // this means blocked in sleepMillis() or sleepNanos()
                LockSupport.getBlocker(runnerThread) == this) {
            runnerThread.interrupt(); // unblock
        }
    }

    private int nextSegmentIndex(int segmentIndex) {
        int permutationIndex = inverseSegmentsPermutation[segmentIndex];
        int nextPermutationIndex = (permutationIndex + 1) % map.segments();
        return segmentsPermutation[nextPermutationIndex];
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
}
