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

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.TimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class OldDeletedEntriesCleanup implements Runnable, Closeable, Predicate<ReplicableEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(OldDeletedEntriesCleanup.class);

    private static final int PRIME_1 = 1_000_003, PRIME_2 = 999_983;

    private final ReplicatedChronicleMap<?, ?, ?, ?, ?, ?, ?> map;
    private final long timeout;
    private final int step;
    private volatile boolean shutdown;
    private long prevSegment0ScanStart = -1;
    private long removedCompletely;

    public OldDeletedEntriesCleanup(ReplicatedChronicleMap<?, ?, ?, ?, ?, ?, ?> map, long timeout) {
        this.map = map;
        this.timeout = timeout;
        // step primality guarantees perfect segment permutation
        step = map.segments() != PRIME_1 ? PRIME_1 : PRIME_2;
    }

    @Override
    public void run() {
        while (!shutdown) {
            int segmentIndex = map.globalMutableState.getCurrentCleanupSegmentIndex();
            TimeProvider timeProvider = map.timeProvider;
            if (segmentIndex == 0 && prevSegment0ScanStart >= 0) {
                long currentTime = timeProvider.currentTime();
                long mapScanTime = currentTime - prevSegment0ScanStart;
                LOG.debug("Old deleted entries scan time: {}", mapScanTime);
                if (mapScanTime < timeout) {
                    long timeToSleep =
                            timeProvider.unscale(timeout - mapScanTime, TimeUnit.MILLISECONDS);
                    if (timeToSleep > 0) {
                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);

                        }
                    } else {
                        timeToSleep = timeProvider.unscale(timeout - mapScanTime,
                                TimeUnit.NANOSECONDS);
                        try {
                            Thread.sleep(0, (int) timeToSleep);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            try (MapSegmentContext<?, ?, ?> context = map.segmentContext(segmentIndex)) {
                if (segmentIndex == 0)
                    prevSegment0ScanStart = timeProvider.currentTime();
                removedCompletely = 0;
                if (((ReplicatedHashSegmentContext<?, ?>) context)
                        .forEachSegmentReplicableEntryWhile(this)) {
                    segmentIndex = (segmentIndex + step) % map.segments();
                    map.globalMutableState.setCurrentCleanupSegmentIndex(segmentIndex);
                }
            }
            LOG.debug("Removed {} old deleted entries in the segment {}",
                    removedCompletely, segmentIndex);
        }
    }

    @Override
    public boolean test(ReplicableEntry e) {
        if (shutdown)
            return false;
        if (e instanceof MapAbsentEntry) {
            long deleteTimeout = map.timeProvider.currentTime() - e.originTimestamp();
            if (deleteTimeout > timeout && !e.isChanged()) {
                e.doRemoveCompletely();
                removedCompletely++;
            }
        }
        return true;
    }

    @Override
    public void close() {
        shutdown = true;
    }
}
