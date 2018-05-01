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
