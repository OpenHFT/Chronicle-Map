/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CleanupRepeatedPassTest {
    @Test
    void backgroundCleanupReclaimsTwoSeparateDeletionBatches() throws Exception {
        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMap.of(Integer.class, Integer.class)
                .entries(32).actualSegments(2).replication((byte) 1);
        ChronicleHashBuilderPrivateAPI<?, ?> config = Jvm.getValue(builder, "privateAPI");
        config.removedEntryCleanupTimeout(1500, TimeUnit.MICROSECONDS);
        try (ChronicleMap<Integer, Integer> map = builder.create()) {
            for (int batch = 0; batch < 2; batch++) {
                for (int i = 0; i < 8; i++) { int key = batch * 10 + i; map.put(key, key); map.remove(key); }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (replicableCount(map) > 0 && System.nanoTime() < deadline) Jvm.pause(10);
                assertEquals(0, replicableCount(map), "background cleanup must process every batch");
            }
        }
    }

    private int replicableCount(ChronicleMap<Integer, Integer> map) {
        int[] count = {0};
        for (int i = 0; i < map.segments(); i++) {
            try (MapSegmentContext<Integer, Integer, ?> context = map.segmentContext(i)) {
                ((ReplicatedHashSegmentContext<Integer, ?>) context).forEachSegmentReplicableEntry(e -> count[0]++);
            }
        }
        return count[0];
    }
}
