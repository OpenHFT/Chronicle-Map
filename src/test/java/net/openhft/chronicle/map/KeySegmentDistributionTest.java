/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class KeySegmentDistributionTest {

    @Test
    public void keySegmentDistributionTestOneSegment() {
        Assertions.assertDoesNotThrow(() -> keySegmentDistributionTest(1000, 1),
                "key segment distribution (1 segment)");
    }

    @Test
    public void keySegmentDistributionTestPowerOfTwoSegments() {
        Assertions.assertDoesNotThrow(() -> keySegmentDistributionTest(1000, 4),
                "key segment distribution (power-of-two segments)");
    }

    @Test
    public void keySegmentDistributionTestOddSegments() {
        Assertions.assertDoesNotThrow(() -> keySegmentDistributionTest(1000, 5),
                "key segment distribution (odd segments)");
    }

    public void keySegmentDistributionTest(int size, int segments) {
        ChronicleMap<CharSequence, Integer> map = ChronicleMapBuilder
                .of(CharSequence.class, Integer.class)
                .actualSegments(segments)
                // TODO problems with computing proper number of segments/chunks
                // when I write this without `* 2` I expect not to have ISE "segment is full"...
                .entries(size * 2)
                .averageKeySize(10)
                .create();

        byte[] keyBytes = new byte[10];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            random.nextBytes(keyBytes);
            String key = new String(keyBytes, StandardCharsets.US_ASCII);
            long hash = LongHashFunction.xx().hashBytes(StandardCharsets.UTF_8.encode(key));
            int segmentIndex = (((int) hash) & Integer.MAX_VALUE) % segments;
            // Put the segment index as a value to the map
            map.put(key, segmentIndex);
        }
        // The following loop checks that internally hash code and segment index is chosen
        // the same way as we explicitly computed in this test. Since ChMap iteration is segment
        // by segment, we expect to see the sequence of values 0, 0, ... 0, 1, ..1, 2, ..2, 3, ..3
        // or opposite -- 3, 3, ... 3, 2, ..2, 1, ..1, 0, ..0
        int currentSegment = -1;
        boolean ascendingDirection = false;
        for (Integer entrySegment : map.values()) {
            if (currentSegment == -1) {
                currentSegment = entrySegment;
                if (currentSegment == 0) {
                    ascendingDirection = true;
                }
            } else {
                if (ascendingDirection) {
                    Assertions.assertTrue(entrySegment >= currentSegment, "entrySegment >= currentSegment");
                    currentSegment = entrySegment;
                } else {
                    // descending iteration direction
                    Assertions.assertTrue(entrySegment <= currentSegment, "entrySegment <= currentSegment");
                    currentSegment = entrySegment;
                }
            }
        }
    }
}
