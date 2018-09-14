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

import net.openhft.hashing.LongHashFunction;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class KeySegmentDistributionTest {

    @Test
    public void keySegmentDistributionTestOneSegment() {
        keySegmentDistributionTest(1000, 1);
    }

    @Test
    public void keySegmentDistributionTestPowerOfTwoSegments() {
        keySegmentDistributionTest(1000, 4);
    }

    @Test
    public void keySegmentDistributionTestOddSegments() {
        keySegmentDistributionTest(1000, 5);
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
                    Assert.assertTrue(entrySegment >= currentSegment);
                    currentSegment = entrySegment;
                } else {
                    // descending iteration direction
                    Assert.assertTrue(entrySegment <= currentSegment);
                    currentSegment = entrySegment;
                }
            }
        }
    }
}
